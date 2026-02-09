package theatre

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"io"
	"net"
	"testing"
	"time"
)

// testEnvelope wraps Envelope for test code. Panics on error.
// Panics are acceptable in test helpers — they surface as test failures.
func testEnvelope(payload interface{}) TransportEnvelope {
	env, err := Envelope(payload)
	if err != nil {
		panic(err)
	}
	return env
}

// --- framing round-trip tests (via net.Pipe) ---

func TestFrameRoundTrip_ActorForward(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()

	original := ActorForward{
		ActorType:    "greeter",
		ActorID:      "abc-123",
		Body:         "hello world",
		ReplyID:      42,
		SenderHostID: "host-a",
	}

	errCh := make(chan error, 1)
	go func() {
		p := &transportPeer{hostID: "test", conn: c1}
		tr := &Transport{} // only needed to call writeFrame
		errCh <- tr.writeFrame(p, testEnvelope(original))
	}()

	env, err := readFrame(c2)
	if err != nil {
		t.Fatalf("readFrame: %v", err)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("writeFrame: %v", err)
	}

	if env.Tag != TagActorForward {
		t.Fatalf("tag: got %d, want %d", env.Tag, TagActorForward)
	}
	got, ok := env.Payload.(*ActorForward)
	if !ok {
		t.Fatalf("payload type: got %T, want *ActorForward", env.Payload)
	}
	if got.ActorType != original.ActorType {
		t.Errorf("ActorType: got %q, want %q", got.ActorType, original.ActorType)
	}
	if got.ActorID != original.ActorID {
		t.Errorf("ActorID: got %q, want %q", got.ActorID, original.ActorID)
	}
	if got.Body != original.Body {
		t.Errorf("Body: got %v, want %v", got.Body, original.Body)
	}
	if got.ReplyID != original.ReplyID {
		t.Errorf("ReplyID: got %d, want %d", got.ReplyID, original.ReplyID)
	}
	if got.SenderHostID != original.SenderHostID {
		t.Errorf("SenderHostID: got %q, want %q", got.SenderHostID, original.SenderHostID)
	}
}

func TestFrameRoundTrip_ActorForwardReply(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()

	original := ActorForwardReply{
		ReplyID: 99,
		Body:    "response payload",
		Error:   "something went wrong",
	}

	errCh := make(chan error, 1)
	go func() {
		p := &transportPeer{hostID: "test", conn: c1}
		tr := &Transport{}
		errCh <- tr.writeFrame(p, testEnvelope(original))
	}()

	env, err := readFrame(c2)
	if err != nil {
		t.Fatalf("readFrame: %v", err)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("writeFrame: %v", err)
	}

	if env.Tag != TagActorForwardReply {
		t.Fatalf("tag: got %d, want %d", env.Tag, TagActorForwardReply)
	}
	got := env.Payload.(*ActorForwardReply)
	if got.ReplyID != original.ReplyID {
		t.Errorf("ReplyID: got %d, want %d", got.ReplyID, original.ReplyID)
	}
	if got.Body != original.Body {
		t.Errorf("Body: got %v, want %v", got.Body, original.Body)
	}
	if got.Error != original.Error {
		t.Errorf("Error: got %q, want %q", got.Error, original.Error)
	}
}

func TestFrameRoundTrip_NotHere(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()

	original := NotHere{
		ActorType: "worker",
		ActorID:   "7",
		HostID:    "host-b",
		Epoch:     5,
	}

	errCh := make(chan error, 1)
	go func() {
		p := &transportPeer{hostID: "test", conn: c1}
		tr := &Transport{}
		errCh <- tr.writeFrame(p, testEnvelope(original))
	}()

	env, err := readFrame(c2)
	if err != nil {
		t.Fatalf("readFrame: %v", err)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("writeFrame: %v", err)
	}

	got := env.Payload.(*NotHere)
	if got.ActorType != original.ActorType || got.ActorID != original.ActorID ||
		got.HostID != original.HostID || got.Epoch != original.Epoch {
		t.Errorf("got %+v, want %+v", got, original)
	}
}

func TestFrameRoundTrip_HostFrozen(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()

	original := HostFrozen{ReplyID: 10, HostID: "host-c", Epoch: 3}

	errCh := make(chan error, 1)
	go func() {
		p := &transportPeer{hostID: "test", conn: c1}
		tr := &Transport{}
		errCh <- tr.writeFrame(p, testEnvelope(original))
	}()

	env, err := readFrame(c2)
	if err != nil {
		t.Fatalf("readFrame: %v", err)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("writeFrame: %v", err)
	}

	got := env.Payload.(*HostFrozen)
	if got.ReplyID != original.ReplyID || got.HostID != original.HostID || got.Epoch != original.Epoch {
		t.Errorf("got %+v, want %+v", got, original)
	}
}

func TestFrameRoundTrip_PingPong(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()

	errCh := make(chan error, 1)
	go func() {
		p := &transportPeer{hostID: "test", conn: c1}
		tr := &Transport{}
		errCh <- tr.writeFrame(p, testEnvelope(TransportPing{}))
	}()

	env, err := readFrame(c2)
	if err != nil {
		t.Fatalf("readFrame: %v", err)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("writeFrame: %v", err)
	}
	if env.Tag != TagPing {
		t.Fatalf("tag: got %d, want %d", env.Tag, TagPing)
	}
	if _, ok := env.Payload.(*TransportPing); !ok {
		t.Fatalf("payload: got %T, want *TransportPing", env.Payload)
	}

	// Now pong.
	c3, c4 := net.Pipe()
	defer c3.Close()
	defer c4.Close()

	go func() {
		p := &transportPeer{hostID: "test", conn: c3}
		tr := &Transport{}
		errCh <- tr.writeFrame(p, testEnvelope(TransportPong{}))
	}()

	env, err = readFrame(c4)
	if err != nil {
		t.Fatalf("readFrame pong: %v", err)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("writeFrame pong: %v", err)
	}
	if env.Tag != TagPong {
		t.Fatalf("tag: got %d, want %d", env.Tag, TagPong)
	}
}

// --- handshake tests ---

func TestHandshakeRoundTrip(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()

	errCh := make(chan error, 1)
	go func() {
		errCh <- writeHandshake(c1, "host-alpha", "127.0.0.1:9000")
	}()

	gotID, gotAddr, err := readHandshake(c2)
	if err != nil {
		t.Fatalf("readHandshake: %v", err)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("writeHandshake: %v", err)
	}
	if gotID != "host-alpha" {
		t.Fatalf("hostID: got %q, want %q", gotID, "host-alpha")
	}
	if gotAddr != "127.0.0.1:9000" {
		t.Fatalf("addr: got %q, want %q", gotAddr, "127.0.0.1:9000")
	}
}

func TestHandshakeRoundTrip_WithAddress(t *testing.T) {
	// Verify that various address values round-trip correctly,
	// including an empty address.
	cases := []struct {
		name   string
		hostID string
		addr   string
	}{
		{"with-address", "host-beta", "10.0.0.1:4000"},
		{"empty-address", "host-gamma", ""},
		{"ipv6-address", "host-delta", "[::1]:8080"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c1, c2 := net.Pipe()
			defer c1.Close()
			defer c2.Close()

			errCh := make(chan error, 1)
			go func() {
				errCh <- writeHandshake(c1, tc.hostID, tc.addr)
			}()

			gotID, gotAddr, err := readHandshake(c2)
			if err != nil {
				t.Fatalf("readHandshake: %v", err)
			}
			if err := <-errCh; err != nil {
				t.Fatalf("writeHandshake: %v", err)
			}
			if gotID != tc.hostID {
				t.Errorf("hostID: got %q, want %q", gotID, tc.hostID)
			}
			if gotAddr != tc.addr {
				t.Errorf("addr: got %q, want %q", gotAddr, tc.addr)
			}
		})
	}
}

func TestTransport_PeerAddressFromHandshake(t *testing.T) {
	// Verify that the inbound peer's stored address is the remote's
	// advertised listen address, not the ephemeral client port.
	received := make(chan struct{}, 1)

	handlerB := func(from string, env TransportEnvelope) {
		if _, ok := env.Payload.(*TransportPing); ok {
			received <- struct{}{}
		}
	}

	tA, err := NewTransport("host-a", "127.0.0.1:0", nil)
	if err != nil {
		t.Fatalf("NewTransport A: %v", err)
	}
	tA.Start()
	defer tA.Stop()

	tB, err := NewTransport("host-b", "127.0.0.1:0", handlerB)
	if err != nil {
		t.Fatalf("NewTransport B: %v", err)
	}
	tB.Start()
	defer tB.Stop()

	// A sends a ping to B, which establishes an outbound connection from A→B
	// and an inbound connection on B from A.
	pingEnv, err := Envelope(TransportPing{})
	if err != nil {
		t.Fatalf("Envelope: %v", err)
	}
	if err := tA.SendTo("host-b", tB.Addr(), pingEnv); err != nil {
		t.Fatalf("SendTo: %v", err)
	}

	select {
	case <-received:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for ping")
	}

	// Check that B's peer entry for "host-a" has A's listen address,
	// not an ephemeral port.
	v, ok := tB.peers.Load("host-a")
	if !ok {
		t.Fatal("host-a not found in tB peers")
	}
	peerA := v.(*transportPeer)
	peerA.mu.Lock()
	addr := peerA.address
	peerA.mu.Unlock()

	if addr != tA.Addr() {
		t.Errorf("peer address: got %q, want %q (tA listen addr)", addr, tA.Addr())
	}
}

// --- simultaneous connect tie-breaking ---

func TestTransport_SimultaneousConnect_TieBreaking(t *testing.T) {
	// When both sides dial each other simultaneously, the higher-ID host
	// keeps its outbound and rejects the inbound. The lower-ID host accepts
	// the inbound. This should converge to one connection per pair with no
	// cascading reconnects.
	receivedA := make(chan struct{}, 10)
	receivedB := make(chan struct{}, 10)

	handlerA := func(from string, env TransportEnvelope) {
		if _, ok := env.Payload.(*TransportPing); ok {
			receivedA <- struct{}{}
		}
	}
	handlerB := func(from string, env TransportEnvelope) {
		if _, ok := env.Payload.(*TransportPing); ok {
			receivedB <- struct{}{}
		}
	}

	// "host-b" > "host-a" lexicographically, so host-b wins tie-breaking.
	tA, err := NewTransport("host-a", "127.0.0.1:0", handlerA)
	if err != nil {
		t.Fatalf("NewTransport A: %v", err)
	}
	tA.Start()
	defer tA.Stop()

	tB, err := NewTransport("host-b", "127.0.0.1:0", handlerB)
	if err != nil {
		t.Fatalf("NewTransport B: %v", err)
	}
	tB.Start()
	defer tB.Stop()

	pingEnv, _ := Envelope(TransportPing{})

	// Trigger simultaneous connect: both sides dial at the same time.
	errCh := make(chan error, 2)
	go func() { errCh <- tA.SendTo("host-b", tB.Addr(), pingEnv) }()
	go func() { errCh <- tB.SendTo("host-a", tA.Addr(), pingEnv) }()

	// Both sends should succeed (possibly after one reconnect cycle).
	for i := 0; i < 2; i++ {
		if err := <-errCh; err != nil {
			t.Fatalf("SendTo %d: %v", i, err)
		}
	}

	// Both sides should receive the ping.
	for i := 0; i < 1; i++ {
		select {
		case <-receivedA:
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for ping on A")
		}
	}
	for i := 0; i < 1; i++ {
		select {
		case <-receivedB:
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for ping on B")
		}
	}

	// Let connections stabilize.
	time.Sleep(100 * time.Millisecond)

	// Send another round — should work without errors on stable connections.
	if err := tA.SendTo("host-b", tB.Addr(), pingEnv); err != nil {
		t.Fatalf("second SendTo A→B: %v", err)
	}
	if err := tB.SendTo("host-a", tA.Addr(), pingEnv); err != nil {
		t.Fatalf("second SendTo B→A: %v", err)
	}

	select {
	case <-receivedB:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for second ping on B")
	}
	select {
	case <-receivedA:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for second ping on A")
	}
}

// --- Envelope error tests ---

func TestEnvelope_ErrorOnUnknown(t *testing.T) {
	_, err := Envelope(struct{ X int }{X: 1})
	if err == nil {
		t.Fatal("expected error for unknown type, got nil")
	}
}

func TestEnvelope_KnownTypes(t *testing.T) {
	cases := []struct {
		name    string
		payload interface{}
		wantTag byte
	}{
		{"ActorForward", ActorForward{}, TagActorForward},
		{"ActorForwardReply", ActorForwardReply{}, TagActorForwardReply},
		{"NotHere", NotHere{}, TagNotHere},
		{"HostFrozen", HostFrozen{}, TagHostFrozen},
		{"Ping", TransportPing{}, TagPing},
		{"Pong", TransportPong{}, TagPong},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env, err := Envelope(tc.payload)
			if err != nil {
				t.Fatalf("Envelope(%s): %v", tc.name, err)
			}
			if env.Tag != tc.wantTag {
				t.Errorf("tag: got %d, want %d", env.Tag, tc.wantTag)
			}
		})
	}
}

// --- full transport integration tests ---

func TestTransport_ForwardAndReply(t *testing.T) {
	replyCh := make(chan *ActorForwardReply, 1)
	forwardCh := make(chan *ActorForward, 1)

	var tB *Transport

	handlerA := func(from string, env TransportEnvelope) {
		if msg, ok := env.Payload.(*ActorForwardReply); ok {
			replyCh <- msg
		}
	}

	handlerB := func(from string, env TransportEnvelope) {
		if msg, ok := env.Payload.(*ActorForward); ok {
			forwardCh <- msg
			// Reply back through the existing inbound connection.
			env, err := Envelope(ActorForwardReply{
				ReplyID: msg.ReplyID,
				Body:    "pong:" + msg.Body.(string),
			})
			if err != nil {
				t.Errorf("Envelope reply: %v", err)
				return
			}
			tB.SendTo(from, "", env)
		}
	}

	tA, err := NewTransport("host-a", "127.0.0.1:0", handlerA)
	if err != nil {
		t.Fatalf("NewTransport A: %v", err)
	}
	tA.Start()
	defer tA.Stop()

	tB, err = NewTransport("host-b", "127.0.0.1:0", handlerB)
	if err != nil {
		t.Fatalf("NewTransport B: %v", err)
	}
	tB.Start()
	defer tB.Stop()

	// A sends forward to B.
	fwdEnv, err := Envelope(ActorForward{
		ActorType:    "greeter",
		ActorID:      "1",
		Body:         "hello",
		ReplyID:      42,
		SenderHostID: "host-a",
	})
	if err != nil {
		t.Fatalf("Envelope forward: %v", err)
	}
	if err := tA.SendTo("host-b", tB.Addr(), fwdEnv); err != nil {
		t.Fatalf("SendTo: %v", err)
	}

	// Verify B received the forward.
	select {
	case fwd := <-forwardCh:
		if fwd.ActorType != "greeter" || fwd.ActorID != "1" {
			t.Errorf("forward: got type=%q id=%q", fwd.ActorType, fwd.ActorID)
		}
		if fwd.ReplyID != 42 {
			t.Errorf("forward ReplyID: got %d, want 42", fwd.ReplyID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for forward")
	}

	// Verify A received the reply with correct correlation.
	select {
	case reply := <-replyCh:
		if reply.ReplyID != 42 {
			t.Errorf("reply ReplyID: got %d, want 42", reply.ReplyID)
		}
		if reply.Body != "pong:hello" {
			t.Errorf("reply Body: got %v, want %q", reply.Body, "pong:hello")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for reply")
	}
}

func TestTransport_PingPong(t *testing.T) {
	pongCh := make(chan struct{}, 1)
	pingCh := make(chan struct{}, 1)

	var tB *Transport

	handlerA := func(from string, env TransportEnvelope) {
		if _, ok := env.Payload.(*TransportPong); ok {
			pongCh <- struct{}{}
		}
	}

	handlerB := func(from string, env TransportEnvelope) {
		if _, ok := env.Payload.(*TransportPing); ok {
			pingCh <- struct{}{}
			env, err := Envelope(TransportPong{})
			if err != nil {
				return
			}
			tB.SendTo(from, "", env)
		}
	}

	tA, err := NewTransport("host-a", "127.0.0.1:0", handlerA)
	if err != nil {
		t.Fatalf("NewTransport A: %v", err)
	}
	tA.Start()
	defer tA.Stop()

	tB, err = NewTransport("host-b", "127.0.0.1:0", handlerB)
	if err != nil {
		t.Fatalf("NewTransport B: %v", err)
	}
	tB.Start()
	defer tB.Stop()

	pingEnv, err := Envelope(TransportPing{})
	if err != nil {
		t.Fatalf("Envelope ping: %v", err)
	}
	if err := tA.SendTo("host-b", tB.Addr(), pingEnv); err != nil {
		t.Fatalf("SendTo ping: %v", err)
	}

	select {
	case <-pingCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for ping")
	}

	select {
	case <-pongCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for pong")
	}
}

func TestTransport_MultipleMessages(t *testing.T) {
	const count = 50
	received := make(chan int64, count)

	handlerB := func(from string, env TransportEnvelope) {
		if msg, ok := env.Payload.(*ActorForward); ok {
			received <- msg.ReplyID
		}
	}

	tA, err := NewTransport("host-a", "127.0.0.1:0", nil)
	if err != nil {
		t.Fatalf("NewTransport A: %v", err)
	}
	tA.Start()
	defer tA.Stop()

	tB, err := NewTransport("host-b", "127.0.0.1:0", handlerB)
	if err != nil {
		t.Fatalf("NewTransport B: %v", err)
	}
	tB.Start()
	defer tB.Stop()

	for i := int64(0); i < count; i++ {
		env, err := Envelope(ActorForward{
			ActorType:    "counter",
			ActorID:      "1",
			Body:         "tick",
			ReplyID:      i,
			SenderHostID: "host-a",
		})
		if err != nil {
			t.Fatalf("Envelope %d: %v", i, err)
		}
		if err := tA.SendTo("host-b", tB.Addr(), env); err != nil {
			t.Fatalf("SendTo %d: %v", i, err)
		}
	}

	seen := make(map[int64]bool)
	for i := 0; i < count; i++ {
		select {
		case id := <-received:
			seen[id] = true
		case <-time.After(5 * time.Second):
			t.Fatalf("timeout after receiving %d/%d messages", i, count)
		}
	}

	if len(seen) != count {
		t.Fatalf("received %d unique messages, want %d", len(seen), count)
	}
}

func TestTransport_CustomBodyType(t *testing.T) {
	type GreetRequest struct {
		Name string
	}
	RegisterGobType(GreetRequest{})

	received := make(chan GreetRequest, 1)

	handlerB := func(from string, env TransportEnvelope) {
		if msg, ok := env.Payload.(*ActorForward); ok {
			if gr, ok := msg.Body.(GreetRequest); ok {
				received <- gr
			}
		}
	}

	tA, err := NewTransport("host-a", "127.0.0.1:0", nil)
	if err != nil {
		t.Fatalf("NewTransport A: %v", err)
	}
	tA.Start()
	defer tA.Stop()

	tB, err := NewTransport("host-b", "127.0.0.1:0", handlerB)
	if err != nil {
		t.Fatalf("NewTransport B: %v", err)
	}
	tB.Start()
	defer tB.Stop()

	env, err := Envelope(ActorForward{
		ActorType:    "greeter",
		ActorID:      "1",
		Body:         GreetRequest{Name: "Alice"},
		ReplyID:      1,
		SenderHostID: "host-a",
	})
	if err != nil {
		t.Fatalf("Envelope: %v", err)
	}
	if err := tA.SendTo("host-b", tB.Addr(), env); err != nil {
		t.Fatalf("SendTo: %v", err)
	}

	select {
	case gr := <-received:
		if gr.Name != "Alice" {
			t.Errorf("Name: got %q, want %q", gr.Name, "Alice")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for custom body message")
	}
}

// --- benchmarks ---

// benchmarkMessages returns the set of envelopes used across benchmarks.
func benchmarkMessages() map[string]TransportEnvelope {
	return map[string]TransportEnvelope{
		"ActorForward": testEnvelope(ActorForward{
			ActorType:    "greeter",
			ActorID:      "abc-123",
			Body:         "hello world",
			ReplyID:      42,
			SenderHostID: "host-a",
		}),
		"ActorForwardReply": testEnvelope(ActorForwardReply{
			ReplyID: 99,
			Body:    "response payload",
			Error:   "something went wrong",
		}),
		"Ping": testEnvelope(TransportPing{}),
	}
}

// encodeFrame encodes an envelope into its wire format (for read-side benchmarks).
func encodeFrame(env TransportEnvelope) []byte {
	var gobBuf bytes.Buffer
	if err := gob.NewEncoder(&gobBuf).Encode(env.Payload); err != nil {
		panic(err)
	}
	gobBytes := gobBuf.Bytes()
	frameLen := 1 + len(gobBytes)
	frame := make([]byte, 4+frameLen)
	binary.BigEndian.PutUint32(frame[:4], uint32(frameLen))
	frame[4] = env.Tag
	copy(frame[5:], gobBytes)
	return frame
}

// BenchmarkWriteFrame measures the encode + frame-build + write path.
// A goroutine drains the read end of the pipe so writes never block.
func BenchmarkWriteFrame(b *testing.B) {
	for name, env := range benchmarkMessages() {
		b.Run(name, func(b *testing.B) {
			c1, c2 := net.Pipe()
			defer c1.Close()

			// Drain reader in background.
			done := make(chan struct{})
			go func() {
				defer close(done)
				io.Copy(io.Discard, c2)
			}()
			defer func() {
				c2.Close()
				<-done
			}()

			p := &transportPeer{hostID: "bench", conn: c1}
			tr := &Transport{}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := tr.writeFrame(p, env); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkReadFrame measures the frame-parse + gob-decode path.
// Pre-encodes frames into a large buffer so reads don't block.
func BenchmarkReadFrame(b *testing.B) {
	for name, env := range benchmarkMessages() {
		b.Run(name, func(b *testing.B) {
			// Pre-encode one frame.
			single := encodeFrame(env)
			b.ReportMetric(float64(len(single)), "bytes/frame")

			// Build a buffer with b.N copies (or a large batch we cycle through).
			const batch = 4096
			var buf bytes.Buffer
			for i := 0; i < batch; i++ {
				buf.Write(single)
			}
			data := buf.Bytes()

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Wrap a reader over the pre-encoded batch, cycling as needed.
				offset := (i % batch) * len(single)
				r := bytes.NewReader(data[offset : offset+len(single)])
				if _, err := readFrame(r); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkRoundTrip measures the full production write + read path through
// a net.Pipe, using per-peer encoder and per-connection decoder (matching
// the readLoop path).
func BenchmarkRoundTrip(b *testing.B) {
	for name, env := range benchmarkMessages() {
		b.Run(name, func(b *testing.B) {
			c1, c2 := net.Pipe()
			defer c1.Close()
			defer c2.Close()

			p := &transportPeer{hostID: "bench", conn: c1}
			tr := &Transport{}

			// Persistent decoder (matches readLoop).
			var decBuf bytes.Buffer
			dec := gob.NewDecoder(&decBuf)

			errCh := make(chan error, 1)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				go func() {
					errCh <- tr.writeFrame(p, env)
				}()
				if _, err := decodeFrame(c2, dec, &decBuf); err != nil {
					b.Fatal(err)
				}
				if err := <-errCh; err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkGobEncode isolates the gob encoding cost (no framing, no IO).
func BenchmarkGobEncode(b *testing.B) {
	for name, env := range benchmarkMessages() {
		b.Run(name, func(b *testing.B) {
			var buf bytes.Buffer
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				buf.Reset()
				if err := gob.NewEncoder(&buf).Encode(env.Payload); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkGobDecode isolates the gob decoding cost (no framing, no IO).
func BenchmarkGobDecode(b *testing.B) {
	for name, env := range benchmarkMessages() {
		b.Run(name, func(b *testing.B) {
			// Pre-encode.
			var buf bytes.Buffer
			if err := gob.NewEncoder(&buf).Encode(env.Payload); err != nil {
				b.Fatal(err)
			}
			encoded := buf.Bytes()
			b.ReportMetric(float64(len(encoded)), "bytes/gob")

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				dec := gob.NewDecoder(bytes.NewReader(encoded))
				switch env.Tag {
				case TagActorForward:
					var v ActorForward
					if err := dec.Decode(&v); err != nil {
						b.Fatal(err)
					}
				case TagActorForwardReply:
					var v ActorForwardReply
					if err := dec.Decode(&v); err != nil {
						b.Fatal(err)
					}
				case TagPing:
					var v TransportPing
					if err := dec.Decode(&v); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}

// BenchmarkDecodeFrame measures the optimized decode path with a persistent
// decoder. Frames are pre-encoded with a persistent encoder (matching the
// production writeFrame path) so steady-state frames omit type descriptions.
func BenchmarkDecodeFrame(b *testing.B) {
	for name, env := range benchmarkMessages() {
		b.Run(name, func(b *testing.B) {
			// Pre-encode a batch of frames using a persistent encoder.
			var encBuf bytes.Buffer
			enc := gob.NewEncoder(&encBuf)

			const batch = 4096
			frames := make([][]byte, batch)
			for i := range frames {
				encBuf.Reset()
				if err := enc.Encode(env.Payload); err != nil {
					b.Fatal(err)
				}
				gobBytes := encBuf.Bytes()
				frameLen := 1 + len(gobBytes)
				frame := make([]byte, 4+frameLen)
				binary.BigEndian.PutUint32(frame[:4], uint32(frameLen))
				frame[4] = env.Tag
				copy(frame[5:], gobBytes)
				frames[i] = frame
			}

			// Report steady-state frame size (frame[1+] after type warmup).
			b.ReportMetric(float64(len(frames[batch-1])), "bytes/frame")

			// Concatenate frames into a single stream for the decoder.
			// First frame has type descriptions; decoder warms up on it.
			var stream bytes.Buffer
			for _, f := range frames {
				stream.Write(f)
			}
			streamBytes := stream.Bytes()

			// Persistent decoder.
			var decBuf bytes.Buffer
			dec := gob.NewDecoder(&decBuf)

			// Warm up: decode first frame (has type info).
			r := bytes.NewReader(streamBytes[:len(frames[0])])
			if _, err := decodeFrame(r, dec, &decBuf); err != nil {
				b.Fatal(err)
			}

			// Build steady-state stream (skip first frame).
			offset0 := len(frames[0])
			steadyBytes := streamBytes[offset0:]

			b.ReportAllocs()
			b.ResetTimer()

			steadyLen := len(steadyBytes)
			frameSize := len(frames[1]) // all steady-state frames are identical
			for i := 0; i < b.N; i++ {
				offset := (i * frameSize) % steadyLen
				if offset+frameSize > steadyLen {
					offset = 0
				}
				r := bytes.NewReader(steadyBytes[offset : offset+frameSize])
				if _, err := decodeFrame(r, dec, &decBuf); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkGobEncodeReuse isolates the gob encoding cost with a reused
// encoder (type cache populated). Compare against BenchmarkGobEncode.
func BenchmarkGobEncodeReuse(b *testing.B) {
	for name, env := range benchmarkMessages() {
		b.Run(name, func(b *testing.B) {
			var buf bytes.Buffer
			enc := gob.NewEncoder(&buf)

			// Warm up: encode one message to populate type cache.
			if err := enc.Encode(env.Payload); err != nil {
				b.Fatal(err)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				buf.Reset()
				if err := enc.Encode(env.Payload); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkGobDecodeReuse isolates the gob decoding cost with a reused
// decoder (type cache populated). Compare against BenchmarkGobDecode.
func BenchmarkGobDecodeReuse(b *testing.B) {
	for name, env := range benchmarkMessages() {
		b.Run(name, func(b *testing.B) {
			// Produce frames with a persistent encoder.
			var encBuf bytes.Buffer
			enc := gob.NewEncoder(&encBuf)

			// Warm-up frame (has type info).
			enc.Encode(env.Payload)
			warmupGob := append([]byte(nil), encBuf.Bytes()...)

			// Steady-state frame (no type info).
			encBuf.Reset()
			enc.Encode(env.Payload)
			steadyGob := append([]byte(nil), encBuf.Bytes()...)

			b.ReportMetric(float64(len(steadyGob)), "bytes/gob")

			// Persistent decoder with warmup.
			var decBuf bytes.Buffer
			dec := gob.NewDecoder(&decBuf)
			decBuf.Write(warmupGob)
			switch env.Tag {
			case TagActorForward:
				var v ActorForward
				dec.Decode(&v)
			case TagActorForwardReply:
				var v ActorForwardReply
				dec.Decode(&v)
			case TagPing:
				var v TransportPing
				dec.Decode(&v)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				decBuf.Reset()
				decBuf.Write(steadyGob)
				switch env.Tag {
				case TagActorForward:
					var v ActorForward
					if err := dec.Decode(&v); err != nil {
						b.Fatal(err)
					}
				case TagActorForwardReply:
					var v ActorForwardReply
					if err := dec.Decode(&v); err != nil {
						b.Fatal(err)
					}
				case TagPing:
					var v TransportPing
					if err := dec.Decode(&v); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}

// BenchmarkFrameSize reports the wire size of each message type (not a speed benchmark).
func BenchmarkFrameSize(b *testing.B) {
	for name, env := range benchmarkMessages() {
		b.Run(name, func(b *testing.B) {
			frame := encodeFrame(env)
			b.ReportMetric(float64(len(frame)), "wire-bytes")
			b.ReportMetric(float64(len(frame)-5), "gob-bytes")
			// Run b.N iterations to satisfy the benchmark framework.
			for i := 0; i < b.N; i++ {
			}
		})
	}
}
