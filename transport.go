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
//     tagBatch frame and writes it directly to the conn in one Write
//     syscall. No bufio layer — frames are already contiguous in memory.
//   - Every conn.Write is bounded by the configured write timeout. On timeout or
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
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// TransportOption configures a Transport. Passed to NewTransport.
type TransportOption func(*transportConfig)

type transportConfig struct {
	dialTimeout      time.Duration // bounds net.DialTimeout when connecting to a peer
	handshakeTimeout time.Duration // bounds the handshake exchange after connection
	readTimeout      time.Duration // deadline for frame reads; detects half-open TCP
	writeTimeout     time.Duration // bounds every conn.Write; required for freeze/drain
	sendBuffer       int           // capacity of each peer's outbound message channel
	maxBatchSize     int           // max messages combined into a single batch frame
	maxFramePayload  int           // upper bound on a single frame's payload size
}

func defaultTransportConfig() transportConfig {
	return transportConfig{
		dialTimeout:      5 * time.Second,
		handshakeTimeout: 5 * time.Second,
		readTimeout:      30 * time.Second,
		writeTimeout:     5 * time.Second,
		sendBuffer:       8192,
		maxBatchSize:     128,
		maxFramePayload:  16 << 20, // 16 MB
	}
}

// WithTransportDialTimeout sets the timeout for dialing peer connections. Default: 5s.
func WithTransportDialTimeout(d time.Duration) TransportOption {
	return func(c *transportConfig) { c.dialTimeout = d }
}

// WithTransportHandshakeTimeout sets the timeout for the handshake exchange
// after a connection is established. Default: 5s.
func WithTransportHandshakeTimeout(d time.Duration) TransportOption {
	return func(c *transportConfig) { c.handshakeTimeout = d }
}

// WithTransportReadTimeout sets the read deadline for inbound frames. Connections
// with no data within this window are torn down. Default: 30s.
func WithTransportReadTimeout(d time.Duration) TransportOption {
	return func(c *transportConfig) { c.readTimeout = d }
}

// WithTransportWriteTimeout sets the timeout for each conn.Write. Default: 5s.
func WithTransportWriteTimeout(d time.Duration) TransportOption {
	return func(c *transportConfig) { c.writeTimeout = d }
}

// WithTransportSendBuffer sets the capacity of each peer's outbound message
// channel. Larger buffers absorb send bursts but consume more memory. Default: 8192.
func WithTransportSendBuffer(n int) TransportOption {
	return func(c *transportConfig) { c.sendBuffer = n }
}

// WithTransportMaxBatchSize sets the maximum number of messages batched into a
// single wire frame. Default: 128.
func WithTransportMaxBatchSize(n int) TransportOption {
	return func(c *transportConfig) { c.maxBatchSize = n }
}

// WithTransportMaxFramePayload sets the upper bound on a single frame's payload
// size. Frames larger than this are rejected on read. Default: 16 MB.
func WithTransportMaxFramePayload(n int) TransportOption {
	return func(c *transportConfig) { c.maxFramePayload = n }
}

// maxBatchCapacity is the compile-time upper bound for stack-allocated batch
// arrays. The runtime config.maxBatchSize controls how many messages are
// actually drained per batch (capped at this value).
const maxBatchCapacity = 128

// TransportHandler is called for every inbound message.
// fromHostID is the remote host that sent the message.
type TransportHandler func(fromHostID string, env TransportEnvelope)

type sendFilterFunc func(string) bool

// Transport manages point-to-point TCP connections between hosts for
// cross-host message delivery.
type Transport struct {
	hostID   string
	listener net.Listener
	config   transportConfig

	peers sync.Map // map[string]*transportPeer

	handler TransportHandler

	// sendFilter is a test-only hook. If non-nil, SendTo calls filter(hostID)
	// before sending. If filter returns false, SendTo returns an error.
	sendFilter atomic.Pointer[sendFilterFunc]

	// dispatchWorkers controls parallel dispatch in readLoop. When > 0,
	// actorForward messages are dispatched to N worker goroutines (sharded
	// by actor ref for ordering), overlapping I/O with handler processing.
	// actorForwardReply messages are always handled inline to minimize
	// reply latency. Default 0 = all messages handled inline.
	dispatchWorkers int

	// sendLanes controls how many parallel send channels (and peerWriter
	// goroutines) each peer gets. Workers shard messages across lanes by
	// actor ref, reducing channel contention and parallelizing encoding.
	// Default 0 or 1 = single channel (original behavior).
	sendLanes int

	// multiConn controls multi-connection mode. When > 0, each send
	// lane gets its own TCP connection, eliminating the mergeCh/flusher
	// bottleneck. Each peerWriter lazily dials its own lane connection.
	// Default 0 = single connection per peer (original behavior).
	multiConn int

	// onError is an optional callback invoked for transport-level errors
	// (connection failures, read/write errors, handshake failures).
	// Set via SetOnError. The callback receives the error message and
	// the remote host ID (if known).
	onError func(message, remoteID, detail string)

	done     chan struct{}
	wg       sync.WaitGroup
	stopOnce sync.Once
}

// SetOnError sets a callback invoked for transport-level errors such as
// connection failures, read/write errors, and handshake failures.
// Must be called before Start.
func (t *Transport) SetOnError(fn func(message, remoteID, detail string)) {
	t.onError = fn
}

// reportError invokes the onError callback if set.
func (t *Transport) reportError(message, remoteID, detail string) {
	if t.onError != nil {
		t.onError(message, remoteID, detail)
	}
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

// SetMultiConn enables multi-connection mode where each send lane gets
// its own TCP connection. Must be called before Start.
func (t *Transport) SetMultiConn(n int) {
	t.multiConn = n
}

func (t *Transport) numLanes() int {
	if t.sendLanes < 1 {
		return 1
	}
	return t.sendLanes
}


// readBufPool recycles byte slices used to read frame payloads.
// Keyed by *[]byte to avoid interface-boxing allocations.
var readBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 256)
		return &b
	},
}


// laneConn holds a per-lane TCP connection in multi-conn mode.
type laneConn struct {
	mu   sync.Mutex
	conn net.Conn
}

type transportPeer struct {
	hostID    string
	address   string
	connected atomic.Bool // lock-free connection check for SendTo fast path

	// Per-peer counters for observability.
	messagesSent     atomic.Int64
	messagesReceived atomic.Int64
	sendErrors       atomic.Int64
	latencyUs        atomic.Int64 // last Ping/Pong RTT in microseconds

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
	laneConns    []laneConn // per-lane connections (multi-conn mode)
	inboundConns []net.Conn // accepted connections (multi-conn mode, guarded by mu)
	writersOnce sync.Once
}

func makeSendChs(n int, multiConn bool, sendBuffer int) ([]chan TransportEnvelope, chan []byte, chan []byte, []laneConn) {
	chs := make([]chan TransportEnvelope, n)
	for i := range chs {
		chs[i] = make(chan TransportEnvelope, sendBuffer)
	}
	if multiConn {
		return chs, nil, nil, make([]laneConn, n)
	}
	var mergeCh, frameFree chan []byte
	if n > 1 {
		mergeCh = make(chan []byte, n*64)
		// Channel-based free list: immune to GC clearing (unlike sync.Pool).
		// Bounded at n*16 slots (~64 for 4 lanes). After warmup, encoders
		// recycle frame slices with zero allocation.
		frameFree = make(chan []byte, n*16)
	}
	return chs, mergeCh, frameFree, nil
}

// NewTransport creates a transport that listens on listenAddr.
// The handler is invoked for every inbound message.
func NewTransport(hostID, listenAddr string, handler TransportHandler, opts ...TransportOption) (*Transport, error) {
	cfg := defaultTransportConfig()
	for _, o := range opts {
		o(&cfg)
	}
	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, fmt.Errorf("transport listen: %w", err)
	}
	return &Transport{
		hostID:   hostID,
		listener: ln,
		handler:  handler,
		config:   cfg,
		done:     make(chan struct{}),
	}, nil
}

// TransportStats holds a snapshot of transport state for observability.
type TransportStats struct {
	Peers          int // number of known peers
	Connections    int // number of connected peers
	SendQueueDepth int // total messages queued across all peers/lanes
}

// Stats returns a snapshot of transport connectivity and queue state.
func (t *Transport) Stats() TransportStats {
	var s TransportStats
	t.peers.Range(func(_, v any) bool {
		p := v.(*transportPeer)
		s.Peers++
		if p.connected.Load() {
			s.Connections++
		}
		for _, ch := range p.sendChs {
			s.SendQueueDepth += len(ch)
		}
		return true
	})
	return s
}

// PeerStats holds a snapshot of a single peer's transport state.
type PeerStats struct {
	HostID           string `json:"host_id"`
	Address          string `json:"address"`
	Connected        bool   `json:"connected"`
	MessagesSent     int64  `json:"messages_sent"`
	MessagesReceived int64  `json:"messages_received"`
	SendErrors       int64  `json:"send_errors"`
	SendQueue        int    `json:"send_queue"`
	LatencyUs        int64  `json:"latency_us"`
}

// PeerSnapshots returns per-peer transport stats for all known peers.
func (t *Transport) PeerSnapshots() []PeerStats {
	var out []PeerStats
	t.peers.Range(func(_, v any) bool {
		p := v.(*transportPeer)
		ps := PeerStats{
			HostID:           p.hostID,
			Address:          p.address,
			Connected:        p.connected.Load(),
			MessagesSent:     p.messagesSent.Load(),
			MessagesReceived: p.messagesReceived.Load(),
			SendErrors:       p.sendErrors.Load(),
			LatencyUs:        p.latencyUs.Load(),
		}
		for _, ch := range p.sendChs {
			ps.SendQueue += len(ch)
		}
		out = append(out, ps)
		return true
	})
	return out
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
	t.wg.Add(1)
	go t.pingLoop()
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
			for _, c := range p.inboundConns {
				c.Close()
			}
			p.mu.Unlock()
			for i := range p.laneConns {
				lc := &p.laneConns[i]
				lc.mu.Lock()
				if lc.conn != nil {
					lc.conn.Close()
					lc.conn = nil
				}
				lc.mu.Unlock()
			}
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
		if p.laneConns != nil {
			// Multi-conn: N peerWriter goroutines, each with own conn.
			for i := range p.sendChs {
				t.wg.Add(1)
				go t.peerWriter(p, i)
			}
		} else if p.mergeCh != nil {
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
// actorForward messages are sharded by actor ref (preserves per-actor ordering).
// actorForwardReply messages are sharded by reply ID.
// Other message types (rare) use lane 0.
func laneFor(env TransportEnvelope, n int) int {
	switch env.Tag {
	case tagActorForward:
		msg := env.Payload.(*actorForward)
		return int(refShard(Ref{Type: msg.ActorType, ID: msg.ActorID}) % uint32(n))
	case tagActorForwardReply:
		msg := env.Payload.(*actorForwardReply)
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
				t.reportError("accept error", "", err.Error())
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
	conn.SetDeadline(time.Now().Add(t.config.handshakeTimeout))

	// Inbound handshake: read → write (opposite of outbound: write → read).
	remoteID, remoteAddr, err := readHandshake(conn)
	if err != nil {
		slog.Error("transport handshake read failed", "error", err)
		t.reportError("handshake read failed", "", err.Error())
		conn.Close()
		return
	}
	if err := writeHandshake(conn, t.hostID, t.Addr()); err != nil {
		slog.Error("transport handshake write failed", "error", err)
		t.reportError("handshake write failed", "", err.Error())
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
		chs, merge, free, lcs := makeSendChs(t.numLanes(), t.multiConn > 0, t.config.sendBuffer)
		newP := &transportPeer{
			hostID:    remoteID,
			address:   peerAddr,
			sendChs:   chs,
			mergeCh:   merge,
			frameFree: free,
			laneConns: lcs,
		}
		actual, _ := t.peers.LoadOrStore(remoteID, newP)
		p = actual.(*transportPeer)
	}

	// Multi-conn mode: accept all inbound connections without
	// tie-breaking. Each lane conn gets its own readLoop.
	if t.multiConn > 0 {
		p.mu.Lock()
		if peerAddr != "" {
			p.address = peerAddr
		}
		p.inboundConns = append(p.inboundConns, conn)
		p.mu.Unlock()
		p.connected.Store(true)
		t.readLoop(remoteID, conn)
		conn.Close()
		p.mu.Lock()
		for i, c := range p.inboundConns {
			if c == conn {
				last := len(p.inboundConns) - 1
				p.inboundConns[i] = p.inboundConns[last]
				p.inboundConns[last] = nil
				p.inboundConns = p.inboundConns[:last]
				break
			}
		}
		p.mu.Unlock()
		return
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
	chs, merge, free, lcs := makeSendChs(t.numLanes(), t.multiConn > 0, t.config.sendBuffer)
	newP := &transportPeer{
		hostID:    hostID,
		address:   address,
		sendChs:   chs,
		mergeCh:   merge,
		frameFree: free,
		laneConns: lcs,
	}
	actual, _ := t.peers.LoadOrStore(hostID, newP)
	p := actual.(*transportPeer)

	// Multi-conn mode: peer entry only, lazy dial per lane.
	if t.multiConn > 0 {
		p.mu.Lock()
		if address != "" {
			p.address = address
		}
		p.mu.Unlock()
		p.connected.Store(true)
		return p, nil
	}

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
	conn, err := net.DialTimeout("tcp", p.address, t.config.dialTimeout)
	if err != nil {
		p.mu.Unlock()
		return nil, fmt.Errorf("transport dial %s (%s): %w", hostID, p.address, err)
	}

	// Set a deadline covering the entire handshake exchange.
	conn.SetDeadline(time.Now().Add(t.config.handshakeTimeout))

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
	useLane := p.laneConns != nil

	// In multi-conn mode, clean up our lane connection on exit.
	if useLane {
		defer func() {
			lc := &p.laneConns[lane]
			lc.mu.Lock()
			if lc.conn != nil {
				lc.conn.Close()
				lc.conn = nil
			}
			lc.mu.Unlock()
		}()
	}

	var (
		frameBuf          []byte
		curConn           net.Conn
		batch             [maxBatchCapacity]TransportEnvelope
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
		for n < t.config.maxBatchSize {
			select {
			case batch[n] = <-ch:
				n++
			default:
				break drain
			}
		}

		// Get connection.
		var conn net.Conn
		if useLane {
			lc := &p.laneConns[lane]
			lc.mu.Lock()
			conn = lc.conn
			if conn == nil {
				var err error
				conn, err = t.dialLane(p, lane)
				if err != nil {
					lc.mu.Unlock()
					recycleEnvelopes(batch[:n])
					continue
				}
				lc.conn = conn
			}
			lc.mu.Unlock()
		} else {
			p.mu.Lock()
			conn = p.conn
			p.mu.Unlock()
		}

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
			conn.SetWriteDeadline(time.Now().Add(t.config.writeTimeout))
			lastWriteDeadline = now
		}

		var writeErr error
		if n == 1 {
			writeErr = writeFrameTo(conn, &frameBuf, batch[0])
		} else {
			writeErr = writeBatchFrameTo(conn, &frameBuf, batch[:n])
		}

		if writeErr != nil {
			p.sendErrors.Add(1)
			t.reportError("write error", p.hostID, writeErr.Error())
			if useLane {
				t.closeLaneConn(p, lane, conn)
			} else {
				t.closePeerConn(p, conn)
			}
			curConn = nil

			// Reconnect.
			if useLane {
				lc := &p.laneConns[lane]
				lc.mu.Lock()
				var err error
				conn, err = t.dialLane(p, lane)
				if err != nil {
					lc.mu.Unlock()
					recycleEnvelopes(batch[:n])
					continue
				}
				lc.conn = conn
				lc.mu.Unlock()
			} else {
				p.mu.Lock()
				conn = p.conn
				p.mu.Unlock()
			}
			if conn == nil {
				recycleEnvelopes(batch[:n])
				continue
			}
			curConn = conn
			lastWriteDeadline = 0
			conn.SetWriteDeadline(time.Now().Add(t.config.writeTimeout))
			lastWriteDeadline = coarseNow.Load()
			if n == 1 {
				writeErr = writeFrameTo(conn, &frameBuf, batch[0])
			} else {
				writeErr = writeBatchFrameTo(conn, &frameBuf, batch[:n])
			}
			if writeErr != nil {
				p.sendErrors.Add(1)
				if useLane {
					t.closeLaneConn(p, lane, conn)
				} else {
					t.closePeerConn(p, conn)
				}
				curConn = nil
				recycleEnvelopes(batch[:n])
				continue
			}
		}

		p.messagesSent.Add(int64(n))
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
	var batch [maxBatchCapacity]TransportEnvelope

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
		for n < t.config.maxBatchSize {
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
	var pendingFrames [maxBatchCapacity][]byte

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
			conn.SetWriteDeadline(time.Now().Add(t.config.writeTimeout))
			lastWriteDeadline = now
		}

		_, writeErr := conn.Write(writeBuf)

		if writeErr != nil {
			p.sendErrors.Add(1)
			t.reportError("write error", p.hostID, writeErr.Error())
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
			conn.SetWriteDeadline(time.Now().Add(t.config.writeTimeout))
			lastWriteDeadline = coarseNow.Load()

			_, writeErr = conn.Write(writeBuf)
			if writeErr != nil {
				p.sendErrors.Add(1)
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

// dialLane dials a new TCP connection for the given send lane in multi-conn
// mode. Performs a full handshake and verifies the remote hostID. Does NOT
// start a readLoop — the remote's handleInbound starts one for each accepted
// connection. Called lazily by peerWriter on first batch or after reconnect.
func (t *Transport) dialLane(p *transportPeer, lane int) (net.Conn, error) {
	conn, err := net.DialTimeout("tcp", p.address, t.config.dialTimeout)
	if err != nil {
		return nil, fmt.Errorf("transport dial lane %d to %s (%s): %w", lane, p.hostID, p.address, err)
	}

	conn.SetDeadline(time.Now().Add(t.config.handshakeTimeout))

	if err := writeHandshake(conn, t.hostID, t.Addr()); err != nil {
		conn.Close()
		return nil, fmt.Errorf("transport lane handshake write: %w", err)
	}

	remoteID, _, err := readHandshake(conn)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("transport lane handshake read: %w", err)
	}

	if remoteID != p.hostID {
		conn.Close()
		return nil, fmt.Errorf("transport lane handshake: expected %q, got %q", p.hostID, remoteID)
	}

	conn.SetDeadline(time.Time{})

	slog.Info("transport lane connected", "remote", p.hostID, "lane", lane)
	return conn, nil
}

// closeLaneConn closes a lane connection and clears it from the peer's
// laneConns slot if it hasn't been replaced.
func (t *Transport) closeLaneConn(p *transportPeer, lane int, conn net.Conn) {
	conn.Close()
	lc := &p.laneConns[lane]
	lc.mu.Lock()
	if lc.conn == conn {
		lc.conn = nil
	}
	lc.mu.Unlock()
}

// --- ping/pong ---

// pingLoop periodically sends a transportPing to all connected peers.
// Runs until t.done is closed.
func (t *Transport) pingLoop() {
	defer t.wg.Done()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-t.done:
			return
		case <-ticker.C:
		}
		now := time.Now().UnixMicro()
		ping := TransportEnvelope{Tag: tagPing, Payload: &transportPing{SentAt: now}}
		t.peers.Range(func(_, v any) bool {
			p := v.(*transportPeer)
			if !p.connected.Load() || len(p.sendChs) == 0 {
				return true
			}
			// Non-blocking send on lane 0 — drop if full.
			select {
			case p.sendChs[0] <- ping:
			default:
			}
			return true
		})
	}
}

// handlePingPong intercepts Ping and Pong messages in readLoop. Returns
// true if the envelope was handled (caller should skip normal dispatch).
func (t *Transport) handlePingPong(p *transportPeer, env TransportEnvelope) bool {
	switch env.Tag {
	case tagPing:
		msg := env.Payload.(*transportPing)
		pong := TransportEnvelope{Tag: tagPong, Payload: &transportPong{EchoedAt: msg.SentAt}}
		if len(p.sendChs) > 0 {
			// Ensure writers are running so the Pong actually gets written.
			p.writersOnce.Do(func() {
				if p.laneConns != nil {
					for i := range p.sendChs {
						t.wg.Add(1)
						go t.peerWriter(p, i)
					}
				} else if p.mergeCh != nil {
					for i := range p.sendChs {
						t.wg.Add(1)
						go t.peerEncoder(p, i)
					}
					t.wg.Add(1)
					go t.peerFlusher(p)
				} else {
					t.wg.Add(1)
					go t.peerWriter(p, 0)
				}
			})
			select {
			case p.sendChs[0] <- pong:
			default:
			}
		}
		return true
	case tagPong:
		msg := env.Payload.(*transportPong)
		if msg.EchoedAt > 0 {
			rtt := time.Now().UnixMicro() - msg.EchoedAt
			if rtt < 1 {
				rtt = 1 // sub-microsecond RTT, clamp to 1
			}
			p.latencyUs.Store(rtt)
		}
		return true
	}
	return false
}

// --- read loop ---

func (t *Transport) readLoop(remoteID string, conn net.Conn) {
	bufReader := bufio.NewReaderSize(conn, 65536)

	// Look up peer once for counter updates and ping/pong handling.
	var peer *transportPeer
	if v, ok := t.peers.Load(remoteID); ok {
		peer = v.(*transportPeer)
	}

	// Throttle read deadline updates: the 30s deadline only needs refreshing
	// every ~10s. Uses the coarse clock (clock.go) for a zero-cost check.
	var lastDeadlineSet int64

	// Reusable batch buffer — avoids allocating a []TransportEnvelope per
	// batch frame. The buffer lives on this goroutine's stack (one readLoop
	// per connection) and is reused across iterations.
	var batchBuf [maxBatchCapacity]TransportEnvelope

	// Dispatch workers: when enabled, actorForward messages are dispatched
	// to parallel workers (sharded by actor ref for ordering), overlapping
	// I/O reads with handler processing. actorForwardReply and control
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
			conn.SetReadDeadline(time.Now().Add(t.config.readTimeout))
			lastDeadlineSet = now
		}
		env, batchN, err := decodeFrameBatch(bufReader, batchBuf[:], t.config.maxFramePayload)
		if err != nil {
			select {
			case <-t.done:
				// shutting down — expected
			default:
				slog.Debug("transport read error", "remote", remoteID, "error", err)
				// Timeouts are expected — they fire when a peer has no
				// traffic within the read deadline window (keepalive).
				// Only report non-timeout errors (EOF, connection reset).
				var ne net.Error
				if !(errors.As(err, &ne) && ne.Timeout()) {
					t.reportError("read error", remoteID, err.Error())
				}
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

		if batchN > 0 {
			if peer != nil {
				peer.messagesReceived.Add(int64(batchN))
			}
			for i := 0; i < batchN; i++ {
				if peer != nil && t.handlePingPong(peer, batchBuf[i]) {
					batchBuf[i] = TransportEnvelope{}
					continue
				}
				if t.handler == nil {
					batchBuf[i] = TransportEnvelope{}
					continue
				}
				if dispatchChs != nil && batchBuf[i].Tag == tagActorForward {
					msg := batchBuf[i].Payload.(*actorForward)
					shard := refShard(Ref{Type: msg.ActorType, ID: msg.ActorID}) % uint32(nWorkers)
					dispatchChs[shard] <- batchBuf[i]
				} else {
					t.handler(remoteID, batchBuf[i])
					recyclePayload(batchBuf[i])
				}
				batchBuf[i] = TransportEnvelope{}
			}
		} else {
			if peer != nil {
				peer.messagesReceived.Add(1)
			}
			if peer != nil && t.handlePingPong(peer, env) {
				continue
			}
			if t.handler == nil {
				continue
			}
			if dispatchChs != nil && env.Tag == tagActorForward {
				msg := env.Payload.(*actorForward)
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
// fast path (actorForward/Reply with string body).
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

// buildBatchFrame encodes multiple envelopes into a single tagBatch frame
// in *frameBuf (no I/O). Encodes directly — no intermediate bytes.Buffer.
func buildBatchFrame(frameBuf *[]byte, envs []TransportEnvelope) error {
	buf := (*frameBuf)[:0]
	buf = append(buf, 0, 0, 0, 0, tagBatch) // 4-byte length placeholder + batch tag

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

// writeBatchFrameTo encodes multiple envelopes into a single tagBatch frame
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

	conn.SetWriteDeadline(time.Now().Add(t.config.writeTimeout))
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
	return decodeFrame(r, defaultTransportConfig().maxFramePayload)
}

// decodeFrame reads a single framed message from r. Each frame is
// self-contained: [4-byte length][1-byte tag][binary-encoded payload].
func decodeFrame(r io.Reader, maxPayload int) (TransportEnvelope, error) {
	// Read 4-byte payload length.
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return TransportEnvelope{}, err
	}
	payloadLen := binary.BigEndian.Uint32(lenBuf[:])
	if payloadLen < 1 {
		return TransportEnvelope{}, fmt.Errorf("transport: frame length %d too small", payloadLen)
	}
	if payloadLen > uint32(maxPayload) {
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
func decodeFrameBatch(r io.Reader, batchBuf []TransportEnvelope, maxPayload int) (env TransportEnvelope, batchN int, err error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return TransportEnvelope{}, 0, err
	}
	payloadLen := binary.BigEndian.Uint32(lenBuf[:])
	if payloadLen < 1 {
		return TransportEnvelope{}, 0, fmt.Errorf("transport: frame length %d too small", payloadLen)
	}
	if payloadLen > uint32(maxPayload) {
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

	if tag == tagBatch {
		n, decErr := decodeBatchInto(data, batchBuf)
		*bp = buf
		readBufPool.Put(bp)
		if decErr != nil {
			return TransportEnvelope{}, 0, fmt.Errorf("transport decode: %w", decErr)
		}
		return TransportEnvelope{Tag: tagBatch}, n, nil
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
