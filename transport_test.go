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

	original := actorForward{
		ActorType:    "greeter",
		ActorID:      "abc-123",
		Body:         "hello world",
		ReplyID:      42,
		SenderHostID: "host-a",
	}

	errCh := make(chan error, 1)
	go func() {
		p := &transportPeer{hostID: "test", conn: c1}
		tr := &Transport{config: defaultTransportConfig()} // only needed to call writeFrame
		errCh <- tr.writeFrame(p, testEnvelope(original))
	}()

	env, err := readFrame(c2)
	if err != nil {
		t.Fatalf("readFrame: %v", err)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("writeFrame: %v", err)
	}

	if env.Tag != tagActorForward {
		t.Fatalf("tag: got %d, want %d", env.Tag, tagActorForward)
	}
	got, ok := env.Payload.(*actorForward)
	if !ok {
		t.Fatalf("payload type: got %T, want *actorForward", env.Payload)
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

	original := actorForwardReply{
		ReplyID: 99,
		Body:    "response payload",
		Error:   "something went wrong",
	}

	errCh := make(chan error, 1)
	go func() {
		p := &transportPeer{hostID: "test", conn: c1}
		tr := &Transport{config: defaultTransportConfig()}
		errCh <- tr.writeFrame(p, testEnvelope(original))
	}()

	env, err := readFrame(c2)
	if err != nil {
		t.Fatalf("readFrame: %v", err)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("writeFrame: %v", err)
	}

	if env.Tag != tagActorForwardReply {
		t.Fatalf("tag: got %d, want %d", env.Tag, tagActorForwardReply)
	}
	got := env.Payload.(*actorForwardReply)
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

	original := notHere{
		ActorType: "worker",
		ActorID:   "7",
		HostID:    "host-b",
		Epoch:     5,
	}

	errCh := make(chan error, 1)
	go func() {
		p := &transportPeer{hostID: "test", conn: c1}
		tr := &Transport{config: defaultTransportConfig()}
		errCh <- tr.writeFrame(p, testEnvelope(original))
	}()

	env, err := readFrame(c2)
	if err != nil {
		t.Fatalf("readFrame: %v", err)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("writeFrame: %v", err)
	}

	got := env.Payload.(*notHere)
	if got.ActorType != original.ActorType || got.ActorID != original.ActorID ||
		got.HostID != original.HostID || got.Epoch != original.Epoch {
		t.Errorf("got %+v, want %+v", got, original)
	}
}

func TestFrameRoundTrip_HostFrozen(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()

	original := hostFrozen{ReplyID: 10, HostID: "host-c", Epoch: 3}

	errCh := make(chan error, 1)
	go func() {
		p := &transportPeer{hostID: "test", conn: c1}
		tr := &Transport{config: defaultTransportConfig()}
		errCh <- tr.writeFrame(p, testEnvelope(original))
	}()

	env, err := readFrame(c2)
	if err != nil {
		t.Fatalf("readFrame: %v", err)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("writeFrame: %v", err)
	}

	got := env.Payload.(*hostFrozen)
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
		tr := &Transport{config: defaultTransportConfig()}
		errCh <- tr.writeFrame(p, testEnvelope(transportPing{}))
	}()

	env, err := readFrame(c2)
	if err != nil {
		t.Fatalf("readFrame: %v", err)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("writeFrame: %v", err)
	}
	if env.Tag != tagPing {
		t.Fatalf("tag: got %d, want %d", env.Tag, tagPing)
	}
	if _, ok := env.Payload.(*transportPing); !ok {
		t.Fatalf("payload: got %T, want *transportPing", env.Payload)
	}

	// Now pong.
	c3, c4 := net.Pipe()
	defer c3.Close()
	defer c4.Close()

	go func() {
		p := &transportPeer{hostID: "test", conn: c3}
		tr := &Transport{config: defaultTransportConfig()}
		errCh <- tr.writeFrame(p, testEnvelope(transportPong{}))
	}()

	env, err = readFrame(c4)
	if err != nil {
		t.Fatalf("readFrame pong: %v", err)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("writeFrame pong: %v", err)
	}
	if env.Tag != tagPong {
		t.Fatalf("tag: got %d, want %d", env.Tag, tagPong)
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
		if env.Tag == tagActorForward {
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

	// A sends a message to B, which establishes an outbound connection from A→B
	// and an inbound connection on B from A.
	fwdEnv, err := Envelope(&actorForward{ActorType: "t", ActorID: "1", Body: "hi", SenderHostID: "host-a"})
	if err != nil {
		t.Fatalf("Envelope: %v", err)
	}
	if err := tA.SendTo("host-b", tB.Addr(), fwdEnv); err != nil {
		t.Fatalf("SendTo: %v", err)
	}

	select {
	case <-received:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message")
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
		if env.Tag == tagActorForward {
			receivedA <- struct{}{}
		}
	}
	handlerB := func(from string, env TransportEnvelope) {
		if env.Tag == tagActorForward {
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

	mkFwd := func(sender string) TransportEnvelope {
		env, _ := Envelope(&actorForward{ActorType: "t", ActorID: "1", Body: "hi", SenderHostID: sender})
		return env
	}

	// Trigger simultaneous connect: both sides dial at the same time.
	errCh := make(chan error, 2)
	go func() { errCh <- tA.SendTo("host-b", tB.Addr(), mkFwd("host-a")) }()
	go func() { errCh <- tB.SendTo("host-a", tA.Addr(), mkFwd("host-b")) }()

	// Both sends should succeed (possibly after one reconnect cycle).
	for i := 0; i < 2; i++ {
		if err := <-errCh; err != nil {
			t.Fatalf("SendTo %d: %v", i, err)
		}
	}

	// Both sides should receive the message.
	for i := 0; i < 1; i++ {
		select {
		case <-receivedA:
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for message on A")
		}
	}
	for i := 0; i < 1; i++ {
		select {
		case <-receivedB:
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for message on B")
		}
	}

	// Let connections stabilize.
	time.Sleep(100 * time.Millisecond)

	// Send another round — should work without errors on stable connections.
	if err := tA.SendTo("host-b", tB.Addr(), mkFwd("host-a")); err != nil {
		t.Fatalf("second SendTo A→B: %v", err)
	}
	if err := tB.SendTo("host-a", tA.Addr(), mkFwd("host-b")); err != nil {
		t.Fatalf("second SendTo B→A: %v", err)
	}

	select {
	case <-receivedB:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for second message on B")
	}
	select {
	case <-receivedA:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for second message on A")
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
		{"actorForward", actorForward{}, tagActorForward},
		{"actorForwardReply", actorForwardReply{}, tagActorForwardReply},
		{"notHere", notHere{}, tagNotHere},
		{"hostFrozen", hostFrozen{}, tagHostFrozen},
		{"Ping", transportPing{}, tagPing},
		{"Pong", transportPong{}, tagPong},
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
	replyCh := make(chan *actorForwardReply, 1)
	forwardCh := make(chan *actorForward, 1)

	var tB *Transport

	handlerA := func(from string, env TransportEnvelope) {
		if msg, ok := env.Payload.(*actorForwardReply); ok {
			cp := *msg // copy before return — readLoop recycles pooled structs
			replyCh <- &cp
		}
	}

	handlerB := func(from string, env TransportEnvelope) {
		if msg, ok := env.Payload.(*actorForward); ok {
			// Build reply BEFORE copying (uses msg fields directly).
			replyEnv, err := Envelope(actorForwardReply{
				ReplyID: msg.ReplyID,
				Body:    "pong:" + msg.Body.(string),
			})
			cp := *msg // copy before return — readLoop recycles pooled structs
			forwardCh <- &cp
			if err != nil {
				t.Errorf("Envelope reply: %v", err)
				return
			}
			tB.SendTo(from, "", replyEnv)
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
	fwdEnv, err := Envelope(actorForward{
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
	// Ping/Pong is now handled automatically by the transport layer.
	// Verify that sending a Ping results in latencyUs being populated
	// on the sending peer after the Pong comes back.

	nopHandler := func(from string, env TransportEnvelope) {}

	tA, err := NewTransport("host-a", "127.0.0.1:0", nopHandler)
	if err != nil {
		t.Fatalf("NewTransport A: %v", err)
	}
	tA.Start()
	defer tA.Stop()

	tB, err := NewTransport("host-b", "127.0.0.1:0", nopHandler)
	if err != nil {
		t.Fatalf("NewTransport B: %v", err)
	}
	tB.Start()
	defer tB.Stop()

	// Establish connection by sending a regular message.
	fwdEnv, _ := Envelope(&actorForward{ActorType: "t", ActorID: "1", Body: "hi", SenderHostID: "host-a"})
	if err := tA.SendTo("host-b", tB.Addr(), fwdEnv); err != nil {
		t.Fatalf("SendTo: %v", err)
	}

	// Small delay to ensure connection is fully established on both sides.
	time.Sleep(100 * time.Millisecond)

	// Send a manual Ping and wait for latencyUs to populate.
	pingEnv, err := Envelope(&transportPing{SentAt: time.Now().UnixMicro()})
	if err != nil {
		t.Fatalf("Envelope ping: %v", err)
	}
	if err := tA.SendTo("host-b", tB.Addr(), pingEnv); err != nil {
		t.Fatalf("SendTo ping: %v", err)
	}

	// Poll for latency to appear on tA's peer entry for host-b.
	deadline := time.After(3 * time.Second)
	for {
		snaps := tA.PeerSnapshots()
		for _, ps := range snaps {
			if ps.HostID == "host-b" && ps.LatencyUs > 0 {
				t.Logf("latency to host-b: %d us", ps.LatencyUs)
				return
			}
		}
		select {
		case <-deadline:
			t.Fatal("timeout waiting for latency to populate")
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func TestTransport_MultipleMessages(t *testing.T) {
	const count = 50
	received := make(chan int64, count)

	handlerB := func(from string, env TransportEnvelope) {
		if msg, ok := env.Payload.(*actorForward); ok {
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
		env, err := Envelope(actorForward{
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
		if msg, ok := env.Payload.(*actorForward); ok {
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

	env, err := Envelope(actorForward{
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
		"actorForward": testEnvelope(actorForward{
			ActorType:    "greeter",
			ActorID:      "abc-123",
			Body:         "hello world",
			ReplyID:      42,
			SenderHostID: "host-a",
		}),
		"actorForwardReply": testEnvelope(actorForwardReply{
			ReplyID: 99,
			Body:    "response payload",
			Error:   "something went wrong",
		}),
		"Ping": testEnvelope(transportPing{}),
	}
}

// encodeFrame encodes an envelope into its wire format (for read-side benchmarks).
func encodeFrame(env TransportEnvelope) []byte {
	var buf bytes.Buffer
	if err := encodePayload(&buf, env); err != nil {
		panic(err)
	}
	payloadBytes := buf.Bytes()
	frameLen := 1 + len(payloadBytes)
	frame := make([]byte, 4+frameLen)
	binary.BigEndian.PutUint32(frame[:4], uint32(frameLen))
	frame[4] = env.Tag
	copy(frame[5:], payloadBytes)
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
			tr := &Transport{config: defaultTransportConfig()}

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
// a net.Pipe.
func BenchmarkRoundTrip(b *testing.B) {
	for name, env := range benchmarkMessages() {
		b.Run(name, func(b *testing.B) {
			c1, c2 := net.Pipe()
			defer c1.Close()
			defer c2.Close()

			p := &transportPeer{hostID: "bench", conn: c1}
			tr := &Transport{config: defaultTransportConfig()}

			errCh := make(chan error, 1)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				go func() {
					errCh <- tr.writeFrame(p, env)
				}()
				if _, err := readFrame(c2); err != nil {
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
				case tagActorForward:
					var v actorForward
					if err := dec.Decode(&v); err != nil {
						b.Fatal(err)
					}
				case tagActorForwardReply:
					var v actorForwardReply
					if err := dec.Decode(&v); err != nil {
						b.Fatal(err)
					}
				case tagPing:
					var v transportPing
					if err := dec.Decode(&v); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}

// BenchmarkDecodeFrame measures the optimized decode path. Frames are
// pre-encoded using the custom binary codec.
func BenchmarkDecodeFrame(b *testing.B) {
	for name, env := range benchmarkMessages() {
		b.Run(name, func(b *testing.B) {
			// Pre-encode a single frame.
			single := encodeFrame(env)
			b.ReportMetric(float64(len(single)), "bytes/frame")

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				r := bytes.NewReader(single)
				if _, err := readFrame(r); err != nil {
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
			case tagActorForward:
				var v actorForward
				dec.Decode(&v)
			case tagActorForwardReply:
				var v actorForwardReply
				dec.Decode(&v)
			case tagPing:
				var v transportPing
				dec.Decode(&v)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				decBuf.Reset()
				decBuf.Write(steadyGob)
				switch env.Tag {
				case tagActorForward:
					var v actorForward
					if err := dec.Decode(&v); err != nil {
						b.Fatal(err)
					}
				case tagActorForwardReply:
					var v actorForwardReply
					if err := dec.Decode(&v); err != nil {
						b.Fatal(err)
					}
				case tagPing:
					var v transportPing
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
