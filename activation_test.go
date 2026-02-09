package theatre

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// reasonCapture records the ActivationReason from Initialize.
type reasonCapture struct {
	mu     sync.Mutex
	reason ActivationReason
	set    bool
	msgs   []interface{}
}

func (r *reasonCapture) Receive(ctx *Context) error {
	switch v := ctx.Message.(type) {
	case Initialize:
		r.mu.Lock()
		r.reason = v.Reason
		r.set = true
		r.mu.Unlock()
		return nil
	case Shutdown:
		return nil
	}
	r.mu.Lock()
	r.msgs = append(r.msgs, ctx.Message)
	r.mu.Unlock()
	if ctx.replyId != 0 {
		ctx.Reply(ctx.Message)
	}
	return nil
}

func (r *reasonCapture) getReason() (ActivationReason, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.reason, r.set
}

func (r *reasonCapture) getMessages() []interface{} {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]interface{}, len(r.msgs))
	copy(out, r.msgs)
	return out
}

// --- tests ---

func TestActivation_GateDedup(t *testing.T) {
	// Verify that concurrent activateActor calls for the same Ref
	// result in only one actor being created.
	h := NewHost(WithIdleTimeout(30 * time.Second))
	h.Start()
	defer h.Stop()

	var captureInstance *reasonCapture
	var captureCount int64

	h.RegisterActor("gated", func() Receiver {
		atomic.AddInt64(&captureCount, 1)
		rc := &reasonCapture{}
		captureInstance = rc
		return rc
	})

	ref := NewRef("gated", "1")

	// Launch N goroutines that all try to activate the same actor.
	const N = 10
	var wg sync.WaitGroup
	actors := make([]*Actor, N)
	errs := make([]error, N)

	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			a, err := h.activateActor(ref, false)
			actors[idx] = a
			errs[idx] = err
		}(i)
	}
	wg.Wait()

	// All should have succeeded with the same actor.
	for i := 0; i < N; i++ {
		if errs[i] != nil {
			t.Fatalf("goroutine %d: unexpected error: %v", i, errs[i])
		}
		if actors[i] == nil {
			t.Fatalf("goroutine %d: actor is nil", i)
		}
	}

	// All should be the same Actor instance.
	for i := 1; i < N; i++ {
		if actors[i] != actors[0] {
			t.Fatalf("goroutine %d returned different actor instance", i)
		}
	}

	// Creator should have been called exactly once.
	if count := atomic.LoadInt64(&captureCount); count != 1 {
		t.Fatalf("expected 1 actor creation, got %d", count)
	}

	// The actor should have received Initialize with ActivationReactivation
	// (because claim=false).
	time.Sleep(50 * time.Millisecond) // let Initialize be processed
	if captureInstance == nil {
		t.Fatal("captureInstance is nil")
	}
	reason, set := captureInstance.getReason()
	if !set {
		t.Fatal("Initialize was not received")
	}
	if reason != ActivationReactivation {
		t.Fatalf("expected ActivationReactivation, got %d", reason)
	}
}

func TestActivation_GateNoDescriptor(t *testing.T) {
	h := NewHost()
	h.Start()
	defer h.Stop()

	// No actor registered → activateActor should fail.
	ref := NewRef("missing", "1")
	a, err := h.activateActor(ref, false)
	if a != nil {
		t.Fatal("expected nil actor for unregistered type")
	}
	if err != ErrUnregisteredActorType {
		t.Fatalf("expected ErrUnregisteredActorType, got %v", err)
	}
}

func TestActivation_InitializeBeforeMessages(t *testing.T) {
	// Verify that Initialize is always the first message processed.
	h := NewHost(WithIdleTimeout(30 * time.Second))
	h.Start()
	defer h.Stop()

	rc := &reasonCapture{}
	h.RegisterActor("ordered", func() Receiver { return rc })

	ref := NewRef("ordered", "1")
	a, err := h.activateActor(ref, false)
	if err != nil {
		t.Fatal(err)
	}

	// Send a business message immediately after activation.
	a.Send(InboxMessage{
		RecipientRef: ref,
		Body:         "msg-1",
	})
	a.Send(InboxMessage{
		RecipientRef: ref,
		Body:         "msg-2",
	})

	// Wait for messages.
	deadline := time.After(2 * time.Second)
	for {
		msgs := rc.getMessages()
		if len(msgs) >= 2 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("timeout waiting for messages")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	// Initialize should have been set (processed before business messages).
	reason, set := rc.getReason()
	if !set {
		t.Fatal("Initialize was not received")
	}
	if reason != ActivationReactivation {
		t.Fatalf("expected ActivationReactivation, got %d", reason)
	}

	msgs := rc.getMessages()
	if msgs[0] != "msg-1" || msgs[1] != "msg-2" {
		t.Fatalf("unexpected message order: %v", msgs)
	}
}

func TestActivation_StandaloneInitializeReason(t *testing.T) {
	// In standalone mode, actors get ActivationNew.
	h := NewHost()
	h.Start()
	defer h.Stop()

	rc := &reasonCapture{}
	h.RegisterActor("standalone", func() Receiver { return rc })

	ref := NewRef("standalone", "1")
	_, err := h.Request(ref, "hello")
	if err != nil {
		t.Fatal(err)
	}

	reason, set := rc.getReason()
	if !set {
		t.Fatal("Initialize was not received")
	}
	if reason != ActivationNew {
		t.Fatalf("expected ActivationNew, got %d", reason)
	}
}

func TestActivation_ReleaseOnStop(t *testing.T) {
	// Verify that releaseOwnership is called when actor stops.
	// Since we have no DB, we just verify the callback fires.
	h := NewHost(WithIdleTimeout(30 * time.Second))
	h.Start()
	defer h.Stop()

	var deactivated int64

	h.RegisterActor("release", func() Receiver { return &echoReceiver{} })

	ref := NewRef("release", "1")
	a := h.createLocalActor(ref, ActivationNew)
	if a == nil {
		t.Fatal("failed to create actor")
	}

	// Override onDeactivate to track calls.
	a.onDeactivate = func(r Ref) {
		atomic.AddInt64(&deactivated, 1)
	}

	// Self-stop.
	a.Send(InboxMessage{
		RecipientRef: ref,
		Body:         "trigger",
		ReplyID:      0,
	})

	// Give it time, then stop.
	time.Sleep(100 * time.Millisecond)

	// Shutdown the actor.
	h.actors.Remove(ref)

	time.Sleep(50 * time.Millisecond)

	if atomic.LoadInt64(&deactivated) == 0 {
		t.Fatal("onDeactivate was not called")
	}
}

func TestActivation_ReleaseOnSelfStop(t *testing.T) {
	// Verify onDeactivate fires when actor returns ErrStopActor.
	h := NewHost(WithIdleTimeout(30 * time.Second))
	h.Start()
	defer h.Stop()

	var deactivated int64

	h.RegisterActor("selfstop", func() Receiver {
		return ReceiverFunc(func(ctx *Context) error {
			switch ctx.Message.(type) {
			case Initialize, Shutdown:
				return nil
			}
			return ErrStopActor
		})
	})

	ref := NewRef("selfstop", "1")
	a := h.createLocalActor(ref, ActivationNew)
	if a == nil {
		t.Fatal("failed to create actor")
	}

	a.onDeactivate = func(r Ref) {
		atomic.AddInt64(&deactivated, 1)
	}

	a.Send(InboxMessage{
		RecipientRef: ref,
		Body:         "stop-me",
	})

	// Wait for the actor to stop.
	time.Sleep(200 * time.Millisecond)

	if atomic.LoadInt64(&deactivated) == 0 {
		t.Fatal("onDeactivate was not called on self-stop")
	}
}

// ReceiverFunc allows using a function as a Receiver.
type ReceiverFunc func(ctx *Context) error

func (f ReceiverFunc) Receive(ctx *Context) error {
	return f(ctx)
}
