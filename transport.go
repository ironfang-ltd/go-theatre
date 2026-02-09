package theatre

// Transport manages point-to-point TCP connections between actor hosts.
//
// Invariants:
//   - At most one logical connection exists between any pair of hosts.
//   - Connections are established lazily on first SendTo call.
//   - Wire format: [4-byte big-endian payload length][1-byte tag][gob payload].
//     Payload length covers the tag byte plus the gob bytes.
//   - A read error tears down the connection; the next SendTo reconnects.
//   - Each peer has a dedicated writer goroutine that reads from a send
//     channel and writes frames. This eliminates write contention — only
//     one goroutine writes to each connection.
//   - The writer goroutine uses a bufio.Writer for automatic batching:
//     multiple frames accumulate in the buffer and flush in a single
//     conn.Write syscall when no more messages are immediately available.
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
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
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
const peerSendBuffer = 4096

// TransportHandler is called for every inbound message.
// fromHostID is the remote host that sent the message.
type TransportHandler func(fromHostID string, env TransportEnvelope)

type Transport struct {
	hostID   string
	listener net.Listener

	peers sync.Map // map[string]*transportPeer

	handler TransportHandler

	// sendFilter is a test-only hook. If non-nil, SendTo calls filter(hostID)
	// before sending. If filter returns false, SendTo returns an error.
	filterMu   sync.RWMutex
	sendFilter func(string) bool

	done     chan struct{}
	wg       sync.WaitGroup
	stopOnce sync.Once
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
	hostID  string
	address string

	mu       sync.Mutex // guards conn lifecycle and encoder (writeFrame compat)
	conn     net.Conn
	outbound bool         // true if we dialed (getOrConnect); false if they dialed (handleInbound)
	enc      *gob.Encoder // per-connection encoder; nil until first write (writeFrame compat)
	encBuf   bytes.Buffer // backing buffer for enc (writeFrame compat)
	frameBuf []byte       // reusable frame buffer (writeFrame compat)

	// Per-peer writer goroutine. SendTo pushes envelopes to sendCh;
	// the peerWriter goroutine reads and writes them with zero contention.
	sendCh    chan TransportEnvelope
	writerOnce sync.Once
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
	t.filterMu.Lock()
	t.sendFilter = fn
	t.filterMu.Unlock()
}

// SendTo sends a message to the specified host. If no connection exists,
// it dials address to establish one. The address is only used for the
// initial dial; subsequent calls for the same hostID reuse the connection.
//
// Messages are queued in a per-peer channel and written by a dedicated
// goroutine, so SendTo returns as soon as the message is enqueued.
func (t *Transport) SendTo(hostID, address string, env TransportEnvelope) error {
	t.filterMu.RLock()
	filter := t.sendFilter
	t.filterMu.RUnlock()
	if filter != nil && !filter(hostID) {
		return fmt.Errorf("transport: send to %s blocked by filter", hostID)
	}

	p, err := t.getOrConnect(hostID, address)
	if err != nil {
		return err
	}

	// Start writer goroutine (once per peer lifetime).
	p.writerOnce.Do(func() {
		t.wg.Add(1)
		go t.peerWriter(p)
	})

	select {
	case p.sendCh <- env:
		return nil
	case <-t.done:
		return fmt.Errorf("transport: shutting down")
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
		newP := &transportPeer{
			hostID:  remoteID,
			address: peerAddr,
			sendCh:  make(chan TransportEnvelope, peerSendBuffer),
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
	p.enc = nil // new connection; reset encoder type cache (writeFrame compat)
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
	// Fast path: peer exists and is connected (lock-free map lookup).
	if v, ok := t.peers.Load(hostID); ok {
		p := v.(*transportPeer)
		p.mu.Lock()
		connected := p.conn != nil
		p.mu.Unlock()
		if connected {
			return p, nil
		}
	}

	// Slow path: create peer entry if needed.
	newP := &transportPeer{
		hostID:  hostID,
		address: address,
		sendCh:  make(chan TransportEnvelope, peerSendBuffer),
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
	p.enc = nil // new connection; reset encoder type cache (writeFrame compat)
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

// peerWriter is the dedicated write goroutine for a single peer.
// It reads envelopes from p.sendCh and writes them to the connection.
// A bufio.Writer batches frames: multiple envelopes are encoded into the
// buffer and flushed in a single conn.Write when no more are immediately
// available. Since only this goroutine writes to the connection, there
// is zero write contention.
func (t *Transport) peerWriter(p *transportPeer) {
	defer t.wg.Done()

	var (
		enc      *gob.Encoder
		encBuf   bytes.Buffer
		frameBuf []byte
		bw       *bufio.Writer
		curConn  net.Conn
	)

	for {
		var env TransportEnvelope
		select {
		case env = <-p.sendCh:
		case <-t.done:
			return
		}

		// Snapshot current connection.
		p.mu.Lock()
		conn := p.conn
		p.mu.Unlock()

		if conn == nil {
			continue // dropped; next SendTo will reconnect
		}

		// Reset encoder and writer on connection change.
		if conn != curConn {
			curConn = conn
			encBuf.Reset()
			enc = gob.NewEncoder(&encBuf)
			bw = bufio.NewWriterSize(conn, 65536)
		}

		conn.SetWriteDeadline(time.Now().Add(transportWriteTimeout))

		// Write first frame.
		if err := writeFrameTo(bw, enc, &encBuf, &frameBuf, env); err != nil {
			t.closePeerConn(p, conn)
			curConn = nil

			// The connection may have been replaced by handleInbound
			// (simultaneous connect tie-breaking). Retry once with
			// the new connection if available.
			p.mu.Lock()
			conn = p.conn
			p.mu.Unlock()
			if conn == nil {
				continue
			}
			curConn = conn
			encBuf.Reset()
			enc = gob.NewEncoder(&encBuf)
			bw = bufio.NewWriterSize(conn, 65536)
			conn.SetWriteDeadline(time.Now().Add(transportWriteTimeout))
			if err := writeFrameTo(bw, enc, &encBuf, &frameBuf, env); err != nil {
				t.closePeerConn(p, conn)
				curConn = nil
				continue
			}
		}

		// Drain additional available messages for batching.
	drain:
		for {
			select {
			case env = <-p.sendCh:
				if err := writeFrameTo(bw, enc, &encBuf, &frameBuf, env); err != nil {
					t.closePeerConn(p, conn)
					curConn = nil
					break drain
				}
			default:
				break drain
			}
		}

		// Flush batch to network.
		if curConn != nil {
			if err := bw.Flush(); err != nil {
				t.closePeerConn(p, conn)
				curConn = nil
			}
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
		p.enc = nil
	}
	p.mu.Unlock()
}

// --- read loop ---

func (t *Transport) readLoop(remoteID string, conn net.Conn) {
	// Per-connection decoder: retains type info across frames so only the
	// first message of a given type pays the type-description overhead.
	var decBuf bytes.Buffer
	dec := gob.NewDecoder(&decBuf)

	// Buffered reader: one 64KB kernel read serves ~640 small frames,
	// eliminating per-frame read syscalls. The raw conn is kept for
	// SetReadDeadline calls.
	bufReader := bufio.NewReaderSize(conn, 65536)

	// Throttle read deadline updates: the 30s deadline only needs refreshing
	// every ~10s. Uses the coarse clock (clock.go) for a zero-cost check.
	var lastDeadlineSet int64

	for {
		now := coarseNow.Load()
		if now-lastDeadlineSet >= 10 {
			conn.SetReadDeadline(time.Now().Add(transportReadTimeout))
			lastDeadlineSet = now
		}
		env, err := decodeFrame(bufReader, dec, &decBuf)
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
					}
					p.mu.Unlock()
				}
			}
			return
		}

		if t.handler != nil {
			t.handler(remoteID, env)
		}
	}
}

// --- framing ---

// writeFrameTo encodes env into a single frame and writes it to w.
// The caller provides the encoder, encode buffer, and frame buffer,
// all of which are reused across calls for the same connection.
func writeFrameTo(w io.Writer, enc *gob.Encoder, encBuf *bytes.Buffer, frameBuf *[]byte, env TransportEnvelope) error {
	encBuf.Reset()
	if err := enc.Encode(env.Payload); err != nil {
		return fmt.Errorf("transport encode: %w", err)
	}
	gobBytes := encBuf.Bytes()

	frameLen := 1 + len(gobBytes)
	needed := 4 + frameLen
	buf := *frameBuf
	if cap(buf) < needed {
		buf = make([]byte, needed)
	} else {
		buf = buf[:needed]
	}
	binary.BigEndian.PutUint32(buf[:4], uint32(frameLen))
	buf[4] = env.Tag
	copy(buf[5:], gobBytes)
	*frameBuf = buf

	_, err := w.Write(buf)
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

	if p.enc == nil {
		p.encBuf.Reset()
		p.enc = gob.NewEncoder(&p.encBuf)
	}

	conn.SetWriteDeadline(time.Now().Add(transportWriteTimeout))
	if err := writeFrameTo(conn, p.enc, &p.encBuf, &p.frameBuf, env); err != nil {
		conn.Close()
		if p.conn == conn {
			p.conn = nil
			p.enc = nil
		}
		return fmt.Errorf("transport write: %w", err)
	}

	return nil
}

// readFrame reads a single framed message from r using a fresh gob decoder.
// Used by tests for simple one-shot round-trips. Production code uses
// decodeFrame with a persistent decoder (see readLoop).
func readFrame(r io.Reader) (TransportEnvelope, error) {
	var decBuf bytes.Buffer
	dec := gob.NewDecoder(&decBuf)
	return decodeFrame(r, dec, &decBuf)
}

// decodeFrame reads a single framed message from r using a persistent gob
// decoder. The decoder retains type information across calls, so only the
// first message of a given type includes the full type description; subsequent
// messages of the same type decode with dramatically fewer allocations.
//
// decBuf must be the same bytes.Buffer that was passed to gob.NewDecoder
// when creating dec. The caller creates both once per connection and reuses
// them for every frame.
func decodeFrame(r io.Reader, dec *gob.Decoder, decBuf *bytes.Buffer) (TransportEnvelope, error) {
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

	// Read [tag][gob payload] into a pooled buffer.
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

	// Feed gob bytes to the persistent decoder's buffer.
	// decBuf.Write copies the data, so we can return buf to the pool.
	decBuf.Reset()
	decBuf.Write(buf[1:])
	*bp = buf
	readBufPool.Put(bp)

	var payload interface{}
	switch tag {
	case TagActorForward:
		var v ActorForward
		if err := dec.Decode(&v); err != nil {
			return TransportEnvelope{}, fmt.Errorf("transport decode ActorForward: %w", err)
		}
		payload = &v
	case TagActorForwardReply:
		var v ActorForwardReply
		if err := dec.Decode(&v); err != nil {
			return TransportEnvelope{}, fmt.Errorf("transport decode ActorForwardReply: %w", err)
		}
		payload = &v
	case TagNotHere:
		var v NotHere
		if err := dec.Decode(&v); err != nil {
			return TransportEnvelope{}, fmt.Errorf("transport decode NotHere: %w", err)
		}
		payload = &v
	case TagHostFrozen:
		var v HostFrozen
		if err := dec.Decode(&v); err != nil {
			return TransportEnvelope{}, fmt.Errorf("transport decode HostFrozen: %w", err)
		}
		payload = &v
	case TagPing:
		var v TransportPing
		if err := dec.Decode(&v); err != nil {
			return TransportEnvelope{}, fmt.Errorf("transport decode Ping: %w", err)
		}
		payload = &v
	case TagPong:
		var v TransportPong
		if err := dec.Decode(&v); err != nil {
			return TransportEnvelope{}, fmt.Errorf("transport decode Pong: %w", err)
		}
		payload = &v
	default:
		return TransportEnvelope{}, fmt.Errorf("transport: unknown tag %d", tag)
	}

	return TransportEnvelope{Tag: tag, Payload: payload}, nil
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
