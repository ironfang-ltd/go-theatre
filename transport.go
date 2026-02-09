package theatre

// Transport manages point-to-point TCP connections between actor hosts.
//
// Invariants:
//   - At most one logical connection exists between any pair of hosts.
//   - Connections are established lazily on first SendTo call.
//   - Wire format: [4-byte big-endian payload length][1-byte tag][gob payload].
//     Payload length covers the tag byte plus the gob bytes.
//   - A read error tears down the connection; the next SendTo reconnects.
//   - writeFrame builds the entire frame in memory and issues a single
//     conn.Write under the peer mutex, so concurrent senders cannot interleave.
//   - Every conn.Write is bounded by transportWriteTimeout. On timeout or
//     error the connection is closed and cleared, allowing reconnect on next send.
//
// Handshake direction:
//   - Outbound (dialer):  write handshake → read handshake
//   - Inbound  (listener): read handshake → write handshake
//   - If both sides connect simultaneously, each side ends up with an
//     inbound connection from the other. The inbound handler replaces any
//     existing outbound connection for that hostID (old conn is closed,
//     its readLoop exits on EOF). This converges to one usable connection
//     per direction without requiring a tie-breaking protocol.

import (
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

// transportWriteTimeout bounds every conn.Write. If the peer stops reading,
// the write will fail after this duration instead of blocking forever.
// This is required for correct freeze/drain behavior.
const transportWriteTimeout = 5 * time.Second

// TransportHandler is called for every inbound message.
// fromHostID is the remote host that sent the message.
type TransportHandler func(fromHostID string, env TransportEnvelope)

type Transport struct {
	hostID   string
	listener net.Listener

	mu    sync.Mutex
	peers map[string]*transportPeer

	handler TransportHandler

	// sendFilter is a test-only hook. If non-nil, SendTo calls filter(hostID)
	// before sending. If filter returns false, SendTo returns an error.
	filterMu   sync.RWMutex
	sendFilter func(string) bool

	done     chan struct{}
	wg       sync.WaitGroup
	stopOnce sync.Once
}

type transportPeer struct {
	hostID  string
	address string

	mu   sync.Mutex // guards conn writes and conn lifecycle
	conn net.Conn
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
		peers:    make(map[string]*transportPeer),
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

		t.mu.Lock()
		for _, p := range t.peers {
			p.mu.Lock()
			if p.conn != nil {
				p.conn.Close()
			}
			p.mu.Unlock()
		}
		t.mu.Unlock()

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
	return t.writeFrame(p, env)
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
// Handshake direction (inbound): read remote hostID first, then send ours.
// This is the mirror of the outbound path in getOrConnect which writes first.
func (t *Transport) handleInbound(conn net.Conn) {
	defer t.wg.Done()

	// Inbound handshake: read → write (opposite of outbound: write → read).
	remoteID, err := readHandshake(conn)
	if err != nil {
		slog.Error("transport handshake read failed", "error", err)
		conn.Close()
		return
	}
	if err := writeHandshake(conn, t.hostID); err != nil {
		slog.Error("transport handshake write failed", "error", err)
		conn.Close()
		return
	}

	slog.Info("transport peer connected", "direction", "inbound", "remote", remoteID)

	// Register the inbound connection as a peer.
	// If an outbound connection already exists for this hostID, the inbound
	// connection replaces it (see file-level comment on simultaneous connect).
	t.mu.Lock()
	p, exists := t.peers[remoteID]
	if !exists {
		p = &transportPeer{hostID: remoteID, address: conn.RemoteAddr().String()}
		t.peers[remoteID] = p
	}
	t.mu.Unlock()

	p.mu.Lock()
	old := p.conn
	p.conn = conn
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
	t.mu.Lock()
	p, exists := t.peers[hostID]
	if !exists {
		p = &transportPeer{hostID: hostID, address: address}
		t.peers[hostID] = p
	}
	t.mu.Unlock()

	p.mu.Lock()
	if p.conn != nil {
		p.mu.Unlock()
		return p, nil
	}

	// Dial and handshake while holding the peer lock so only one goroutine
	// connects at a time. The readLoop goroutine is started after unlocking
	// to avoid a deadlock if the first read fails immediately.
	conn, err := net.Dial("tcp", address)
	if err != nil {
		p.mu.Unlock()
		return nil, fmt.Errorf("transport dial %s (%s): %w", hostID, address, err)
	}

	// Outbound handshake: write → read (opposite of inbound: read → write).
	if err := writeHandshake(conn, t.hostID); err != nil {
		conn.Close()
		p.mu.Unlock()
		return nil, fmt.Errorf("transport handshake: %w", err)
	}

	remoteID, err := readHandshake(conn)
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

	p.conn = conn
	p.mu.Unlock() // unlock before starting the read goroutine

	slog.Info("transport peer connected", "direction", "outbound", "remote", hostID, "address", address)

	t.wg.Add(1)
	go func() {
		defer t.wg.Done()
		t.readLoop(hostID, conn)
	}()

	return p, nil
}

// --- read loop ---

func (t *Transport) readLoop(remoteID string, conn net.Conn) {
	for {
		env, err := readFrame(conn)
		if err != nil {
			select {
			case <-t.done:
				// shutting down — expected
			default:
				slog.Warn("transport read error", "remote", remoteID, "error", err)
				// Clear the connection so the next SendTo reconnects.
				t.mu.Lock()
				if p, ok := t.peers[remoteID]; ok {
					p.mu.Lock()
					if p.conn == conn {
						p.conn = nil
					}
					p.mu.Unlock()
				}
				t.mu.Unlock()
			}
			return
		}

		if t.handler != nil {
			t.handler(remoteID, env)
		}
	}
}

// --- framing ---

// writeFrame encodes env into a single frame and writes it atomically
// (single conn.Write) while holding the peer's write lock.
//
// A write deadline of transportWriteTimeout is set before each write and
// cleared on success. If the write times out or fails, the connection is
// closed and cleared so the next SendTo will reconnect.
func (t *Transport) writeFrame(p *transportPeer, env TransportEnvelope) error {
	// Encode the payload.
	var gobBuf bytes.Buffer
	if err := gob.NewEncoder(&gobBuf).Encode(env.Payload); err != nil {
		return fmt.Errorf("transport encode: %w", err)
	}
	gobBytes := gobBuf.Bytes()

	// Build the complete frame: [4-byte length][1-byte tag][gob bytes]
	frameLen := 1 + len(gobBytes)
	frame := make([]byte, 4+frameLen)
	binary.BigEndian.PutUint32(frame[:4], uint32(frameLen))
	frame[4] = env.Tag
	copy(frame[5:], gobBytes)

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.conn == nil {
		return fmt.Errorf("transport: peer %s not connected", p.hostID)
	}

	p.conn.SetWriteDeadline(time.Now().Add(transportWriteTimeout))
	if _, err := p.conn.Write(frame); err != nil {
		p.conn.Close()
		p.conn = nil
		return fmt.Errorf("transport write: %w", err)
	}
	p.conn.SetWriteDeadline(time.Time{}) // clear deadline on success

	return nil
}

// readFrame reads a single framed message from r.
// On return, Payload is a pointer to the decoded concrete type
// (e.g. *ActorForward, *ActorForwardReply, etc.).
func readFrame(r io.Reader) (TransportEnvelope, error) {
	// Read 4-byte payload length.
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return TransportEnvelope{}, err
	}
	payloadLen := binary.BigEndian.Uint32(lenBuf[:])
	if payloadLen < 1 {
		return TransportEnvelope{}, fmt.Errorf("transport: frame length %d too small", payloadLen)
	}
	const maxFrameSize = 16 << 20 // 16 MB
	if payloadLen > maxFrameSize {
		return TransportEnvelope{}, fmt.Errorf("transport: frame too large (%d bytes)", payloadLen)
	}

	// Read [tag][gob payload].
	buf := make([]byte, payloadLen)
	if _, err := io.ReadFull(r, buf); err != nil {
		return TransportEnvelope{}, fmt.Errorf("transport: incomplete frame: %w", err)
	}

	tag := buf[0]
	gobBytes := buf[1:]
	dec := gob.NewDecoder(bytes.NewReader(gobBytes))

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
// Handshake format: [2-byte big-endian hostID length][hostID UTF-8 bytes]
// Max hostID length: 256 bytes.
//
// Direction symmetry:
//   - Outbound (getOrConnect): write our hostID → read remote hostID
//   - Inbound  (handleInbound): read remote hostID → write our hostID
//
// This asymmetry is intentional. The dialer writes first because it knows
// who it expects to reach; the listener reads first to learn who connected.
// On simultaneous connect both sides get an inbound connection that replaces
// the outbound one (old conn closed, its readLoop exits on EOF).

func writeHandshake(w io.Writer, hostID string) error {
	id := []byte(hostID)
	buf := make([]byte, 2+len(id))
	binary.BigEndian.PutUint16(buf[:2], uint16(len(id)))
	copy(buf[2:], id)
	_, err := w.Write(buf)
	return err
}

func readHandshake(r io.Reader) (string, error) {
	var lenBuf [2]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return "", fmt.Errorf("handshake read length: %w", err)
	}
	n := binary.BigEndian.Uint16(lenBuf[:])
	if n == 0 || n > 256 {
		return "", fmt.Errorf("handshake: invalid hostID length %d", n)
	}
	id := make([]byte, n)
	if _, err := io.ReadFull(r, id); err != nil {
		return "", fmt.Errorf("handshake read hostID: %w", err)
	}
	return string(id), nil
}
