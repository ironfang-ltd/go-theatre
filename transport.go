package theatre

// Transport manages point-to-point TCP connections between actor hosts.
//
// Invariants:
//   - At most one logical connection exists between any pair of hosts.
//   - Connections are established lazily on first SendTo call.
//   - Wire format: [4-byte big-endian payload length][1-byte tag][binary payload].
//     Payload length covers the tag byte plus the encoded bytes.
//   - A read error tears down the connection; the next SendTo reconnects.
//   - Each peer has a dedicated writer goroutine that reads from a send
//     channel and writes frames. This eliminates write contention — only
//     one goroutine writes to each connection.
//   - The writer goroutine batches multiple envelopes into a single
//     TagBatch frame and writes it directly to the conn in one Write
//     syscall. No bufio layer — frames are already contiguous in memory.
//   - Every conn.Write is bounded by transportWriteTimeout. On timeout or
//     error the connection is closed and cleared, allowing reconnect on next send.
//   - conn.Read uses a 64KB bufio.Reader. Read deadlines are refreshed every
//     ~10s (not per frame) using the coarse clock, detecting half-open TCP.
//
// Handshake format:
//
//	[2-byte big-endian hostID length][hostID UTF-8 bytes]
//	[2-byte big-endian addr length][addr UTF-8 bytes]
//
// The addr field carries the sender's advertised listen address so the
// receiver stores it for future outbound dials (instead of the ephemeral
// client port from conn.RemoteAddr()).
//
// Handshake direction:
//   - Outbound (dialer):  write handshake → read handshake
//   - Inbound  (listener): read handshake → write handshake
//   - Both dial and handshake are bounded by dedicated timeouts.
//   - If both sides connect simultaneously, deterministic tie-breaking
//     prevents cascading reconnects: the host with the lexicographically
//     higher hostID keeps its outbound connection and rejects the inbound;
//     the lower-ID host accepts the inbound, replacing its outbound.
//     This converges to exactly one connection per peer pair in one round.

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// transportDialTimeout bounds net.DialTimeout when connecting to a peer.
const transportDialTimeout = 5 * time.Second

// transportHandshakeTimeout bounds the handshake exchange (read + write)
// after a connection is established. Prevents slow/malicious peers from
// holding a connection indefinitely before identifying themselves.
const transportHandshakeTimeout = 5 * time.Second

// transportReadTimeout is the deadline for each frame read. If no data
// arrives within this window the connection is torn down. This detects
// half-open TCP connections and acts as a natural idle-connection reaper.
// The next SendTo call reconnects automatically.
const transportReadTimeout = 30 * time.Second

// transportWriteTimeout bounds every conn.Write. If the peer stops reading,
// the write will fail after this duration instead of blocking forever.
// This is required for correct freeze/drain behavior.
const transportWriteTimeout = 5 * time.Second

// peerSendBuffer is the capacity of each peer's outbound message channel.
const peerSendBuffer = 8192

// maxBatchSize is the maximum number of messages combined into a single
// batch frame on the wire. The peerWriter drains up to this many messages
// before encoding them as one TagBatch frame (or a single frame if N==1).
const maxBatchSize = 128

// TransportHandler is called for every inbound message.
// fromHostID is the remote host that sent the message.
type TransportHandler func(fromHostID string, env TransportEnvelope)

type sendFilterFunc func(string) bool

type Transport struct {
	hostID   string
	listener net.Listener

	peers sync.Map // map[string]*transportPeer

	handler TransportHandler

	// sendFilter is a test-only hook. If non-nil, SendTo calls filter(hostID)
	// before sending. If filter returns false, SendTo returns an error.
	sendFilter atomic.Pointer[sendFilterFunc]

	// dispatchWorkers controls parallel dispatch in readLoop. When > 0,
	// ActorForward messages are dispatched to N worker goroutines (sharded
	// by actor ref for ordering), overlapping I/O with handler processing.
	// ActorForwardReply messages are always handled inline to minimize
	// reply latency. Default 0 = all messages handled inline.
	dispatchWorkers int

	// sendLanes controls how many parallel send channels (and peerWriter
	// goroutines) each peer gets. Workers shard messages across lanes by
	// actor ref, reducing channel contention and parallelizing encoding.
	// Default 0 or 1 = single channel (original behavior).
	sendLanes int

	done     chan struct{}
	wg       sync.WaitGroup
	stopOnce sync.Once
}

// SetDispatchWorkers configures the number of parallel dispatch workers
// per readLoop. Must be called before Start. See Transport.dispatchWorkers.
func (t *Transport) SetDispatchWorkers(n int) {
	t.dispatchWorkers = n
}

// SetSendLanes configures the number of parallel send channels per peer.
// Must be called before Start. See Transport.sendLanes.
func (t *Transport) SetSendLanes(n int) {
	if n < 1 {
		n = 1
	}
	t.sendLanes = n
}

func (t *Transport) numLanes() int {
	if t.sendLanes < 1 {
		return 1
	}
	return t.sendLanes
}

// maxFramePayload is the upper bound on a single frame's payload
// (tag byte + gob bytes). Frames larger than this are rejected on read.
const maxFramePayload = 16 << 20 // 16 MB

// readBufPool recycles byte slices used to read frame payloads.
// Keyed by *[]byte to avoid interface-boxing allocations.
var readBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 256)
		return &b
	},
}


type transportPeer struct {
	hostID    string
	address   string
	connected atomic.Bool // lock-free connection check for SendTo fast path

	mu       sync.Mutex // guards conn lifecycle (writeFrame compat)
	conn     net.Conn
	outbound bool   // true if we dialed (getOrConnect); false if they dialed (handleInbound)
	frameBuf []byte // reusable frame buffer (writeFrame compat)

	// Sharded send lanes. Workers shard messages across N sendChs to
	// reduce channel contention. Each lane has a peerEncoder goroutine
	// that encodes batches into frame bytes and sends them to mergeCh.
	// A single peerFlusher goroutine reads from mergeCh and writes all
	// accumulated frames in one conn.Write, preserving batch efficiency.
	// With 1 lane, the encoder writes directly to conn (no mergeCh).
	sendChs   []chan TransportEnvelope
	mergeCh   chan []byte // encoded frames from encoders → flusher
	frameFree chan []byte // channel-based free list for encoder↔flusher frame reuse
	writersOnce sync.Once
}

func makeSendChs(n int) ([]chan TransportEnvelope, chan []byte, chan []byte) {
	chs := make([]chan TransportEnvelope, n)
	for i := range chs {
		chs[i] = make(chan TransportEnvelope, peerSendBuffer)
	}
	var mergeCh, frameFree chan []byte
	if n > 1 {
		mergeCh = make(chan []byte, n*64)
		// Channel-based free list: immune to GC clearing (unlike sync.Pool).
		// Bounded at n*16 slots (~64 for 4 lanes). After warmup, encoders
		// recycle frame slices with zero allocation.
		frameFree = make(chan []byte, n*16)
	}
	return chs, mergeCh, frameFree
}

// NewTransport creates a transport that listens on listenAddr.
// The handler is invoked for every inbound message.
func NewTransport(hostID, listenAddr string, handler TransportHandler) (*Transport, error) {
	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, fmt.Errorf("transport listen: %w", err)
	}
	return &Transport{
		hostID:   hostID,
		listener: ln,
		handler:  handler,
		done:     make(chan struct{}),
	}, nil
}

// Addr returns the listener's network address (useful when binding to ":0").
func (t *Transport) Addr() string {
	return t.listener.Addr().String()
}

// PeerAddress returns the stored address for a connected peer.
// Returns "" if the peer is unknown. Lock-free (sync.Map load).
func (t *Transport) PeerAddress(hostID string) string {
	if v, ok := t.peers.Load(hostID); ok {
		return v.(*transportPeer).address
	}
	return ""
}

// Start begins accepting inbound connections. Non-blocking.
func (t *Transport) Start() {
	t.wg.Add(1)
	go t.acceptLoop()
}

// Stop closes all connections and the listener, then waits for goroutines to exit.
// Safe to call multiple times (idempotent via sync.Once).
func (t *Transport) Stop() {
	t.stopOnce.Do(func() {
		close(t.done)
		t.listener.Close()

		t.peers.Range(func(key, value any) bool {
			p := value.(*transportPeer)
			p.mu.Lock()
			if p.conn != nil {
				p.conn.Close()
			}
			p.mu.Unlock()
			return true
		})

		t.wg.Wait()
	})
}

// SetSendFilter installs a function called before every SendTo. If the
// filter returns false for a hostID, SendTo returns an error. Pass nil
// to remove the filter. Test-only; used in chaos tests to simulate
// network partitions between specific peers.
func (t *Transport) SetSendFilter(fn func(string) bool) {
	if fn == nil {
		t.sendFilter.Store(nil)
	} else {
		f := sendFilterFunc(fn)
		t.sendFilter.Store(&f)
	}
}

// SendTo sends a message to the specified host. If no connection exists,
// it dials address to establish one. The address is only used for the
// initial dial; subsequent calls for the same hostID reuse the connection.
//
// Messages are queued in a per-peer channel and written by a dedicated
// goroutine, so SendTo returns as soon as the message is enqueued.
func (t *Transport) SendTo(hostID, address string, env TransportEnvelope) error {
	if fp := t.sendFilter.Load(); fp != nil && !(*fp)(hostID) {
		return fmt.Errorf("transport: send to %s blocked by filter", hostID)
	}

	p, err := t.getOrConnect(hostID, address)
	if err != nil {
		return err
	}

	// Start encoder + flusher goroutines (once per peer lifetime).
	p.writersOnce.Do(func() {
		if p.mergeCh != nil {
			// Multi-lane: N encoders feed a single flusher.
			for i := range p.sendChs {
				t.wg.Add(1)
				go t.peerEncoder(p, i)
			}
			t.wg.Add(1)
			go t.peerFlusher(p)
		} else {
			// Single lane: direct writer (encode + write in one goroutine).
			t.wg.Add(1)
			go t.peerWriter(p, 0)
		}
	})

	// Select lane based on envelope content.
	ch := p.sendChs[0]
	if n := len(p.sendChs); n > 1 {
		ch = p.sendChs[laneFor(env, n)]
	}

	// Fast path: non-blocking send when buffer has space (avoids
	// the overhead of a two-case select on every message).
	select {
	case ch <- env:
		return nil
	default:
	}
	// Slow path: channel full or shutting down.
	select {
	case ch <- env:
		return nil
	case <-t.done:
		return fmt.Errorf("transport: shutting down")
	}
}

// laneFor returns the send lane index for a given envelope.
// ActorForward messages are sharded by actor ref (preserves per-actor ordering).
// ActorForwardReply messages are sharded by reply ID.
// Other message types (rare) use lane 0.
func laneFor(env TransportEnvelope, n int) int {
	switch env.Tag {
	case TagActorForward:
		msg := env.Payload.(*ActorForward)
		return int(refShard(Ref{Type: msg.ActorType, ID: msg.ActorID}) % uint32(n))
	case TagActorForwardReply:
		msg := env.Payload.(*ActorForwardReply)
		return int(uint32(msg.ReplyID) % uint32(n))
	default:
		return 0
	}
}

// --- accept loop ---

func (t *Transport) acceptLoop() {
	defer t.wg.Done()
	for {
		conn, err := t.listener.Accept()
		if err != nil {
			select {
			case <-t.done:
				return
			default:
				slog.Error("transport accept error", "error", err)
				continue
			}
		}
		t.wg.Add(1)
		go t.handleInbound(conn)
	}
}

// handleInbound processes a new inbound TCP connection.
//
// Handshake direction (inbound): read remote hostID+addr first, then send ours.
// This is the mirror of the outbound path in getOrConnect which writes first.
func (t *Transport) handleInbound(conn net.Conn) {
	defer t.wg.Done()

	// Set a deadline covering the entire handshake exchange.
	conn.SetDeadline(time.Now().Add(transportHandshakeTimeout))

	// Inbound handshake: read → write (opposite of outbound: write → read).
	remoteID, remoteAddr, err := readHandshake(conn)
	if err != nil {
		slog.Error("transport handshake read failed", "error", err)
		conn.Close()
		return
	}
	if err := writeHandshake(conn, t.hostID, t.Addr()); err != nil {
		slog.Error("transport handshake write failed", "error", err)
		conn.Close()
		return
	}

	// Clear the handshake deadline; readLoop sets per-frame deadlines.
	conn.SetDeadline(time.Time{})

	slog.Info("transport peer connected", "direction", "inbound", "remote", remoteID)

	// Use the advertised listen address from the handshake (not the
	// ephemeral client port from conn.RemoteAddr()). This is the address
	// we would need to dial back to reach this peer.
	peerAddr := remoteAddr
	if peerAddr == "" {
		peerAddr = conn.RemoteAddr().String()
	}

	// Register the inbound connection as a peer.
	var p *transportPeer
	if v, ok := t.peers.Load(remoteID); ok {
		p = v.(*transportPeer)
	} else {
		chs, merge, free := makeSendChs(t.numLanes())
		newP := &transportPeer{
			hostID:    remoteID,
			address:   peerAddr,
			sendChs:   chs,
			mergeCh:   merge,
			frameFree: free,
		}
		actual, _ := t.peers.LoadOrStore(remoteID, newP)
		p = actual.(*transportPeer)
	}

	p.mu.Lock()

	// Simultaneous connect tie-breaking: when both sides dial each other,
	// each receives an inbound from the other. Without tie-breaking, both
	// sides replace their outbound with the inbound, causing cascading
	// reconnects. Instead, the host with the higher hostID wins: it keeps
	// its outbound for writing but drains the inbound (reads any data the
	// remote already sent through it). The draining readLoop exits once
	// the remote closes its end (after it accepts our inbound and replaces
	// its own outbound). The lower-ID host accepts the inbound normally.
	if p.conn != nil && p.outbound && t.hostID > remoteID {
		if peerAddr != "" {
			p.address = peerAddr
		}
		p.mu.Unlock()
		slog.Info("transport simultaneous connect (keeping outbound, draining inbound)",
			"remote", remoteID)
		t.readLoop(remoteID, conn)
		conn.Close()
		return
	}

	old := p.conn
	p.conn = conn
	p.outbound = false
	p.connected.Store(true)
	if peerAddr != "" {
		p.address = peerAddr // update address on reconnect
	}
	p.mu.Unlock()

	if old != nil {
		old.Close()
	}

	t.readLoop(remoteID, conn)
}

// --- outbound connect ---

// getOrConnect returns an existing peer or dials a new connection.
//
// Handshake direction (outbound): write our hostID first, then read theirs.
// This is the mirror of the inbound path in handleInbound which reads first.
func (t *Transport) getOrConnect(hostID, address string) (*transportPeer, error) {
	// Fast path: peer exists and is connected (lock-free check).
	if v, ok := t.peers.Load(hostID); ok {
		p := v.(*transportPeer)
		if p.connected.Load() {
			return p, nil
		}
	}

	// Slow path: create peer entry if needed.
	chs, merge, free := makeSendChs(t.numLanes())
	newP := &transportPeer{
		hostID:    hostID,
		address:   address,
		sendChs:   chs,
		mergeCh:   merge,
		frameFree: free,
	}
	actual, _ := t.peers.LoadOrStore(hostID, newP)
	p := actual.(*transportPeer)

	p.mu.Lock()
	if p.conn != nil {
		p.mu.Unlock()
		return p, nil
	}

	// Update address if provided (may differ from initial creation).
	if address != "" {
		p.address = address
	}

	// Dial and handshake while holding the peer lock so only one goroutine
	// connects at a time. The readLoop goroutine is started after unlocking
	// to avoid a deadlock if the first read fails immediately.
	conn, err := net.DialTimeout("tcp", p.address, transportDialTimeout)
	if err != nil {
		p.mu.Unlock()
		return nil, fmt.Errorf("transport dial %s (%s): %w", hostID, p.address, err)
	}

	// Set a deadline covering the entire handshake exchange.
	conn.SetDeadline(time.Now().Add(transportHandshakeTimeout))

	// Outbound handshake: write → read (opposite of inbound: read → write).
	if err := writeHandshake(conn, t.hostID, t.Addr()); err != nil {
		conn.Close()
		p.mu.Unlock()
		return nil, fmt.Errorf("transport handshake: %w", err)
	}

	remoteID, _, err := readHandshake(conn)
	if err != nil {
		conn.Close()
		p.mu.Unlock()
		return nil, fmt.Errorf("transport handshake: %w", err)
	}

	if remoteID != hostID {
		conn.Close()
		p.mu.Unlock()
		return nil, fmt.Errorf("transport handshake: expected host %q, got %q", hostID, remoteID)
	}

	// Clear the handshake deadline; readLoop sets per-frame deadlines.
	conn.SetDeadline(time.Time{})

	p.conn = conn
	p.outbound = true
	p.connected.Store(true)
	p.mu.Unlock()

	slog.Info("transport peer connected", "direction", "outbound", "remote", hostID, "address", address)

	t.wg.Add(1)
	go func() {
		defer t.wg.Done()
		t.readLoop(hostID, conn)
	}()

	return p, nil
}

// --- per-peer writer goroutine ---

// peerWriter is the combined encode+write goroutine for a single-lane peer.
// Used when sendLanes <= 1 (no merge channel). Reads envelopes from
// p.sendChs[0], encodes batches, and writes directly to the connection.
func (t *Transport) peerWriter(p *transportPeer, lane int) {
	defer t.wg.Done()

	ch := p.sendChs[lane]

	var (
		frameBuf          []byte
		curConn           net.Conn
		batch             [maxBatchSize]TransportEnvelope
		lastWriteDeadline int64
	)

	for {
		select {
		case batch[0] = <-ch:
		default:
			select {
			case batch[0] = <-ch:
			case <-t.done:
				return
			}
		}
		n := 1

	drain:
		for n < maxBatchSize {
			select {
			case batch[n] = <-ch:
				n++
			default:
				break drain
			}
		}

		p.mu.Lock()
		conn := p.conn
		p.mu.Unlock()

		if conn == nil {
			recycleEnvelopes(batch[:n])
			continue
		}

		if conn != curConn {
			curConn = conn
			lastWriteDeadline = 0
		}

		now := coarseNow.Load()
		if now-lastWriteDeadline >= 2 {
			conn.SetWriteDeadline(time.Now().Add(transportWriteTimeout))
			lastWriteDeadline = now
		}

		var writeErr error
		if n == 1 {
			writeErr = writeFrameTo(conn, &frameBuf, batch[0])
		} else {
			writeErr = writeBatchFrameTo(conn, &frameBuf, batch[:n])
		}

		if writeErr != nil {
			t.closePeerConn(p, conn)
			curConn = nil

			p.mu.Lock()
			conn = p.conn
			p.mu.Unlock()
			if conn == nil {
				recycleEnvelopes(batch[:n])
				continue
			}
			curConn = conn
			lastWriteDeadline = 0
			conn.SetWriteDeadline(time.Now().Add(transportWriteTimeout))
			lastWriteDeadline = coarseNow.Load()
			if n == 1 {
				writeErr = writeFrameTo(conn, &frameBuf, batch[0])
			} else {
				writeErr = writeBatchFrameTo(conn, &frameBuf, batch[:n])
			}
			if writeErr != nil {
				t.closePeerConn(p, conn)
				curConn = nil
				recycleEnvelopes(batch[:n])
				continue
			}
		}

		recycleEnvelopes(batch[:n])
	}
}

// peerEncoder is an encode-only goroutine for multi-lane peers. It reads
// envelopes from p.sendChs[lane], encodes them into complete frame bytes,
// and sends the frame to p.mergeCh for the flusher to write.
//
// Frame reuse: instead of allocating a new []byte per batch, the encoder
// sends its frameBuf directly on mergeCh (ownership transfer) and gets a
// replacement from p.frameFree (channel-based free list). The flusher
// returns consumed frames to the free list. After warmup, zero allocations.
func (t *Transport) peerEncoder(p *transportPeer, lane int) {
	defer t.wg.Done()

	ch := p.sendChs[lane]

	// Get initial frameBuf from free list (or allocate).
	frameBuf := getFrameBuf(p.frameFree)
	var batch [maxBatchSize]TransportEnvelope

	for {
		select {
		case batch[0] = <-ch:
		default:
			select {
			case batch[0] = <-ch:
			case <-t.done:
				return
			}
		}
		n := 1

	drain:
		for n < maxBatchSize {
			select {
			case batch[n] = <-ch:
				n++
			default:
				break drain
			}
		}

		var err error
		if n == 1 {
			err = buildFrame(&frameBuf, batch[0])
		} else {
			err = buildBatchFrame(&frameBuf, batch[:n])
		}

		recycleEnvelopes(batch[:n])

		if err != nil {
			continue
		}

		// Transfer ownership of frameBuf to the flusher. Get a new one.
		select {
		case p.mergeCh <- frameBuf:
			frameBuf = getFrameBuf(p.frameFree)
		case <-t.done:
			return
		}
	}
}

// getFrameBuf returns a []byte from the free list, or allocates a new one.
func getFrameBuf(free chan []byte) []byte {
	select {
	case buf := <-free:
		return buf[:0]
	default:
		return make([]byte, 0, 4096)
	}
}

// putFrameBuf returns a []byte to the free list (best-effort, drops if full).
func putFrameBuf(free chan []byte, buf []byte) {
	select {
	case free <- buf:
	default:
	}
}

// peerFlusher is the single write goroutine for multi-lane peers. It reads
// pre-encoded frames from p.mergeCh, accumulates them, and writes them to
// the connection in one conn.Write call. This preserves batch efficiency
// (few large writes) while allowing parallel encoding across lanes.
func (t *Transport) peerFlusher(p *transportPeer) {
	defer t.wg.Done()

	var (
		writeBuf          []byte
		curConn           net.Conn
		lastWriteDeadline int64
	)

	// Temporary slice to track frames for returning to the free list.
	var pendingFrames [maxBatchSize][]byte

	for {
		// Block until the first encoded frame arrives.
		var frame []byte
		select {
		case frame = <-p.mergeCh:
		case <-t.done:
			return
		}
		writeBuf = append(writeBuf[:0], frame...)
		pendingFrames[0] = frame
		nFrames := 1

		// Drain more encoded frames (non-blocking).
	drain:
		for {
			select {
			case f := <-p.mergeCh:
				writeBuf = append(writeBuf, f...)
				if nFrames < len(pendingFrames) {
					pendingFrames[nFrames] = f
					nFrames++
				}
			default:
				break drain
			}
		}

		// Snapshot current connection.
		p.mu.Lock()
		conn := p.conn
		p.mu.Unlock()

		if conn == nil {
			for i := range nFrames {
				putFrameBuf(p.frameFree, pendingFrames[i])
				pendingFrames[i] = nil
			}
			continue
		}

		if conn != curConn {
			curConn = conn
			lastWriteDeadline = 0
		}

		now := coarseNow.Load()
		if now-lastWriteDeadline >= 2 {
			conn.SetWriteDeadline(time.Now().Add(transportWriteTimeout))
			lastWriteDeadline = now
		}

		_, writeErr := conn.Write(writeBuf)

		if writeErr != nil {
			t.closePeerConn(p, conn)
			curConn = nil

			p.mu.Lock()
			conn = p.conn
			p.mu.Unlock()
			if conn == nil {
				for i := range nFrames {
					putFrameBuf(p.frameFree, pendingFrames[i])
					pendingFrames[i] = nil
				}
				continue
			}
			curConn = conn
			lastWriteDeadline = 0
			conn.SetWriteDeadline(time.Now().Add(transportWriteTimeout))
			lastWriteDeadline = coarseNow.Load()

			_, writeErr = conn.Write(writeBuf)
			if writeErr != nil {
				t.closePeerConn(p, conn)
				curConn = nil
			}
		}

		// Return all consumed frames to the free list.
		for i := range nFrames {
			putFrameBuf(p.frameFree, pendingFrames[i])
			pendingFrames[i] = nil
		}
	}
}

// closePeerConn closes a connection and clears it from the peer if it
// hasn't been replaced in the meantime.
func (t *Transport) closePeerConn(p *transportPeer, conn net.Conn) {
	conn.Close()
	p.mu.Lock()
	if p.conn == conn {
		p.conn = nil
		p.connected.Store(false)
	}
	p.mu.Unlock()
}

// --- read loop ---

func (t *Transport) readLoop(remoteID string, conn net.Conn) {
	bufReader := bufio.NewReaderSize(conn, 65536)

	// Throttle read deadline updates: the 30s deadline only needs refreshing
	// every ~10s. Uses the coarse clock (clock.go) for a zero-cost check.
	var lastDeadlineSet int64

	// Reusable batch buffer — avoids allocating a []TransportEnvelope per
	// batch frame. The buffer lives on this goroutine's stack (one readLoop
	// per connection) and is reused across iterations.
	var batchBuf [maxBatchSize]TransportEnvelope

	// Dispatch workers: when enabled, ActorForward messages are dispatched
	// to parallel workers (sharded by actor ref for ordering), overlapping
	// I/O reads with handler processing. ActorForwardReply and control
	// messages are always handled inline to minimize reply latency.
	nWorkers := t.dispatchWorkers
	var dispatchChs []chan TransportEnvelope
	if nWorkers > 0 {
		var dwg sync.WaitGroup
		dispatchChs = make([]chan TransportEnvelope, nWorkers)
		for i := range nWorkers {
			dispatchChs[i] = make(chan TransportEnvelope, 512)
			dwg.Add(1)
			go func(ch chan TransportEnvelope) {
				defer dwg.Done()
				for env := range ch {
					t.handler(remoteID, env)
					recyclePayload(env)
				}
			}(dispatchChs[i])
		}
		defer func() {
			for _, ch := range dispatchChs {
				close(ch)
			}
			dwg.Wait()
		}()
	}

	for {
		now := coarseNow.Load()
		if now-lastDeadlineSet >= 10 {
			conn.SetReadDeadline(time.Now().Add(transportReadTimeout))
			lastDeadlineSet = now
		}
		env, batchN, err := decodeFrameBatch(bufReader, batchBuf[:])
		if err != nil {
			select {
			case <-t.done:
				// shutting down — expected
			default:
				slog.Warn("transport read error", "remote", remoteID, "error", err)
				// Clear the connection so the next SendTo reconnects.
				if v, ok := t.peers.Load(remoteID); ok {
					p := v.(*transportPeer)
					p.mu.Lock()
					if p.conn == conn {
						p.conn = nil
						p.connected.Store(false)
					}
					p.mu.Unlock()
				}
			}
			return
		}

		if t.handler == nil {
			continue
		}

		if batchN > 0 {
			for i := 0; i < batchN; i++ {
				if dispatchChs != nil && batchBuf[i].Tag == TagActorForward {
					msg := batchBuf[i].Payload.(*ActorForward)
					shard := refShard(Ref{Type: msg.ActorType, ID: msg.ActorID}) % uint32(nWorkers)
					dispatchChs[shard] <- batchBuf[i]
				} else {
					t.handler(remoteID, batchBuf[i])
					recyclePayload(batchBuf[i])
				}
				batchBuf[i] = TransportEnvelope{}
			}
		} else {
			if dispatchChs != nil && env.Tag == TagActorForward {
				msg := env.Payload.(*ActorForward)
				shard := refShard(Ref{Type: msg.ActorType, ID: msg.ActorID}) % uint32(nWorkers)
				dispatchChs[shard] <- env
			} else {
				t.handler(remoteID, env)
				recyclePayload(env)
			}
		}
	}
}

// --- framing ---

// buildFrame encodes env into a single frame in *frameBuf (no I/O).
// Encodes directly into frameBuf — no intermediate bytes.Buffer for the
// fast path (ActorForward/Reply with string body).
func buildFrame(frameBuf *[]byte, env TransportEnvelope) error {
	buf := (*frameBuf)[:0]
	buf = append(buf, 0, 0, 0, 0, env.Tag) // 4-byte length placeholder + tag

	var err error
	buf, err = appendEncodedPayload(buf, env)
	if err != nil {
		return fmt.Errorf("transport encode: %w", err)
	}

	binary.BigEndian.PutUint32(buf[:4], uint32(len(buf)-4))
	*frameBuf = buf
	return nil
}

// buildBatchFrame encodes multiple envelopes into a single TagBatch frame
// in *frameBuf (no I/O). Encodes directly — no intermediate bytes.Buffer.
func buildBatchFrame(frameBuf *[]byte, envs []TransportEnvelope) error {
	buf := (*frameBuf)[:0]
	buf = append(buf, 0, 0, 0, 0, TagBatch) // 4-byte length placeholder + batch tag

	var err error
	buf, err = appendBatchEncodedPayload(buf, envs)
	if err != nil {
		return fmt.Errorf("transport batch encode: %w", err)
	}

	binary.BigEndian.PutUint32(buf[:4], uint32(len(buf)-4))
	*frameBuf = buf
	return nil
}

// writeFrameTo encodes env into a single frame and writes it to w.
func writeFrameTo(w io.Writer, frameBuf *[]byte, env TransportEnvelope) error {
	if err := buildFrame(frameBuf, env); err != nil {
		return err
	}
	_, err := w.Write(*frameBuf)
	return err
}

// writeBatchFrameTo encodes multiple envelopes into a single TagBatch frame
// and writes it to w.
func writeBatchFrameTo(w io.Writer, frameBuf *[]byte, envs []TransportEnvelope) error {
	if err := buildBatchFrame(frameBuf, envs); err != nil {
		return err
	}
	_, err := w.Write(*frameBuf)
	return err
}

// writeFrame encodes env into a single frame and writes it atomically
// (single conn.Write) while holding the peer's write lock.
//
// This method is retained for backward compatibility with tests and
// benchmarks that create bare transportPeer structs. Production code
// uses the peerWriter goroutine with writeFrameTo instead.
func (t *Transport) writeFrame(p *transportPeer, env TransportEnvelope) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.conn == nil {
		return fmt.Errorf("transport: peer %s not connected", p.hostID)
	}

	conn := p.conn

	conn.SetWriteDeadline(time.Now().Add(transportWriteTimeout))
	if err := writeFrameTo(conn, &p.frameBuf, env); err != nil {
		conn.Close()
		if p.conn == conn {
			p.conn = nil
			p.connected.Store(false)
		}
		return fmt.Errorf("transport write: %w", err)
	}

	return nil
}

// readFrame reads a single framed message from r.
// Used by tests for simple one-shot round-trips.
func readFrame(r io.Reader) (TransportEnvelope, error) {
	return decodeFrame(r)
}

// decodeFrame reads a single framed message from r. Each frame is
// self-contained: [4-byte length][1-byte tag][binary-encoded payload].
func decodeFrame(r io.Reader) (TransportEnvelope, error) {
	// Read 4-byte payload length.
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return TransportEnvelope{}, err
	}
	payloadLen := binary.BigEndian.Uint32(lenBuf[:])
	if payloadLen < 1 {
		return TransportEnvelope{}, fmt.Errorf("transport: frame length %d too small", payloadLen)
	}
	if payloadLen > maxFramePayload {
		return TransportEnvelope{}, fmt.Errorf("transport: frame too large (%d bytes)", payloadLen)
	}

	// Read [tag][payload] into a pooled buffer.
	bp := readBufPool.Get().(*[]byte)
	buf := *bp
	if cap(buf) < int(payloadLen) {
		buf = make([]byte, payloadLen)
	} else {
		buf = buf[:payloadLen]
	}
	if _, err := io.ReadFull(r, buf); err != nil {
		*bp = buf
		readBufPool.Put(bp)
		return TransportEnvelope{}, fmt.Errorf("transport: incomplete frame: %w", err)
	}

	tag := buf[0]
	payload, err := decodePayload(tag, buf[1:])

	*bp = buf
	readBufPool.Put(bp)

	if err != nil {
		return TransportEnvelope{}, fmt.Errorf("transport decode: %w", err)
	}

	return TransportEnvelope{Tag: tag, Payload: payload}, nil
}

// decodeFrameBatch reads a single frame from r. For batch frames it
// decodes sub-messages directly into batchBuf (zero allocation), returning
// the count. For non-batch frames it returns the envelope with batchN==0.
func decodeFrameBatch(r io.Reader, batchBuf []TransportEnvelope) (env TransportEnvelope, batchN int, err error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return TransportEnvelope{}, 0, err
	}
	payloadLen := binary.BigEndian.Uint32(lenBuf[:])
	if payloadLen < 1 {
		return TransportEnvelope{}, 0, fmt.Errorf("transport: frame length %d too small", payloadLen)
	}
	if payloadLen > maxFramePayload {
		return TransportEnvelope{}, 0, fmt.Errorf("transport: frame too large (%d bytes)", payloadLen)
	}

	bp := readBufPool.Get().(*[]byte)
	buf := *bp
	if cap(buf) < int(payloadLen) {
		buf = make([]byte, payloadLen)
	} else {
		buf = buf[:payloadLen]
	}
	if _, err := io.ReadFull(r, buf); err != nil {
		*bp = buf
		readBufPool.Put(bp)
		return TransportEnvelope{}, 0, fmt.Errorf("transport: incomplete frame: %w", err)
	}

	tag := buf[0]
	data := buf[1:]

	if tag == TagBatch {
		n, decErr := decodeBatchInto(data, batchBuf)
		*bp = buf
		readBufPool.Put(bp)
		if decErr != nil {
			return TransportEnvelope{}, 0, fmt.Errorf("transport decode: %w", decErr)
		}
		return TransportEnvelope{Tag: TagBatch}, n, nil
	}

	payload, decErr := decodePayload(tag, data)
	*bp = buf
	readBufPool.Put(bp)
	if decErr != nil {
		return TransportEnvelope{}, 0, fmt.Errorf("transport decode: %w", decErr)
	}
	return TransportEnvelope{Tag: tag, Payload: payload}, 0, nil
}

// --- handshake ---
//
// Handshake format:
//
//	[2-byte big-endian hostID length][hostID UTF-8 bytes]
//	[2-byte big-endian addr length][addr UTF-8 bytes]
//
// Max hostID length: 256 bytes. addr length 0 is valid (empty address).
//
// The addr field carries the sender's advertised listen address so the
// receiver can store it for future outbound connections. Without this,
// inbound connections would only know the ephemeral client port from
// conn.RemoteAddr(), which is useless for dialing back.
//
// Direction symmetry:
//   - Outbound (getOrConnect): write our hostID+addr → read remote hostID+addr
//   - Inbound  (handleInbound): read remote hostID+addr → write our hostID+addr
//
// This asymmetry is intentional. The dialer writes first because it knows
// who it expects to reach; the listener reads first to learn who connected.
// On simultaneous connect, deterministic tie-breaking (higher hostID wins)
// ensures exactly one connection survives per peer pair.

func writeHandshake(w io.Writer, hostID, advertiseAddr string) error {
	id := []byte(hostID)
	addr := []byte(advertiseAddr)
	buf := make([]byte, 2+len(id)+2+len(addr))
	binary.BigEndian.PutUint16(buf[:2], uint16(len(id)))
	copy(buf[2:], id)
	off := 2 + len(id)
	binary.BigEndian.PutUint16(buf[off:off+2], uint16(len(addr)))
	copy(buf[off+2:], addr)
	_, err := w.Write(buf)
	return err
}

func readHandshake(r io.Reader) (hostID, advertiseAddr string, err error) {
	var lenBuf [2]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return "", "", fmt.Errorf("handshake read length: %w", err)
	}
	n := binary.BigEndian.Uint16(lenBuf[:])
	if n == 0 || n > 256 {
		return "", "", fmt.Errorf("handshake: invalid hostID length %d", n)
	}
	id := make([]byte, n)
	if _, err := io.ReadFull(r, id); err != nil {
		return "", "", fmt.Errorf("handshake read hostID: %w", err)
	}

	// Read advertised address.
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return "", "", fmt.Errorf("handshake read addr length: %w", err)
	}
	addrLen := binary.BigEndian.Uint16(lenBuf[:])
	var addr []byte
	if addrLen > 0 {
		addr = make([]byte, addrLen)
		if _, err := io.ReadFull(r, addr); err != nil {
			return "", "", fmt.Errorf("handshake read addr: %w", err)
		}
	}

	return string(id), string(addr), nil
}
