package theatre

import (
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
		errCh <- writeHandshake(c1, "host-alpha")
	}()

	got, err := readHandshake(c2)
	if err != nil {
		t.Fatalf("readHandshake: %v", err)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("writeHandshake: %v", err)
	}
	if got != "host-alpha" {
		t.Fatalf("hostID: got %q, want %q", got, "host-alpha")
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
