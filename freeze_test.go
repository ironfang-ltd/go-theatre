package theatre

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// slowReceiver processes messages with a configurable delay.
type slowReceiver struct {
	mu        sync.Mutex
	received  []interface{}
	delay     time.Duration
	onReceive func()
}

func (r *slowReceiver) Receive(ctx *Context) error {
	switch ctx.Message.(type) {
	case Initialize, Shutdown:
		return nil
	}
	if r.delay > 0 {
		time.Sleep(r.delay)
	}
	r.mu.Lock()
	r.received = append(r.received, ctx.Message)
	r.mu.Unlock()
	if r.onReceive != nil {
		r.onReceive()
	}
	if ctx.replyId != 0 {
		ctx.Reply(ctx.Message)
	}
	return nil
}

func (r *slowReceiver) messages() []interface{} {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]interface{}, len(r.received))
	copy(out, r.received)
	return out
}

// stuckReceiver blocks forever on non-system messages, simulating
// a misbehaving actor that ignores context cancellation.
type stuckReceiver struct {
	started chan struct{}
	block   chan struct{} // closed to unblock; never closed = blocks forever
}

func (r *stuckReceiver) Receive(ctx *Context) error {
	switch ctx.Message.(type) {
	case Initialize:
		if r.started != nil {
			close(r.started)
		}
		return nil
	case Shutdown:
		return nil
	}
	// Block until test unblocks or forever (simulates stuck actor).
	<-r.block
	return nil
}

// --- freeze tests ---

func TestFreeze_RejectsSend(t *testing.T) {
	h := NewHost(WithFreezeGracePeriod(100 * time.Millisecond))
	h.Start()
	defer h.Stop()

	h.RegisterActor("test", func() Receiver { return &echoReceiver{} })

	h.Freeze()

	ref := NewRef("test", "1")
	err := h.Send(ref, "hello")
	if !errors.Is(err, ErrHostFrozen) {
		t.Fatalf("expected ErrHostFrozen from Send, got %v", err)
	}
}

func TestFreeze_RejectsRequest(t *testing.T) {
	h := NewHost(WithFreezeGracePeriod(100 * time.Millisecond))
	h.Start()
	defer h.Stop()

	h.RegisterActor("test", func() Receiver { return &echoReceiver{} })

	h.Freeze()

	ref := NewRef("test", "1")
	_, err := h.Request(ref, "hello")
	if !errors.Is(err, ErrHostFrozen) {
		t.Fatalf("expected ErrHostFrozen from Request, got %v", err)
	}
}

func TestFreeze_IsFrozen(t *testing.T) {
	h := NewHost(WithFreezeGracePeriod(100 * time.Millisecond))
	h.Start()
	defer h.Stop()

	if h.IsFrozen() {
		t.Fatal("expected not frozen initially")
	}

	h.Freeze()

	if !h.IsFrozen() {
		t.Fatal("expected frozen after Freeze()")
	}
}

func TestFreeze_ContextCancellation(t *testing.T) {
	h := NewHost(
		WithFreezeGracePeriod(2*time.Second),
		WithIdleTimeout(30*time.Second),
	)
	h.Start()
	defer h.Stop()

	var ctxCancelled atomic.Bool

	h.RegisterActor("ctx-test", func() Receiver {
		return ReceiverFunc(func(ctx *Context) error {
			switch ctx.Message.(type) {
			case Initialize:
				// Watch for context cancellation in a goroutine.
				go func() {
					<-ctx.Ctx.Done()
					ctxCancelled.Store(true)
				}()
				return nil
			case Shutdown:
				return nil
			}
			if ctx.replyId != 0 {
				ctx.Reply(ctx.Message)
			}
			return nil
		})
	})

	ref := NewRef("ctx-test", "1")
	_, err := h.Request(ref, "activate")
	if err != nil {
		t.Fatal(err)
	}

	h.Freeze()

	// Give a moment for goroutine to observe cancellation.
	time.Sleep(50 * time.Millisecond)

	if !ctxCancelled.Load() {
		t.Fatal("expected actor context to be cancelled after freeze")
	}
}

func TestFreeze_UnblocksInFlightRequests(t *testing.T) {
	h := NewHost(
		WithFreezeGracePeriod(100*time.Millisecond),
		WithIdleTimeout(30*time.Second),
		WithRequestTimeout(10*time.Second),
	)
	h.Start()
	defer h.Stop()

	// Use a slow actor to hold a request in flight.
	h.RegisterActor("slow", func() Receiver {
		return &slowReceiver{delay: 5 * time.Second}
	})

	ref := NewRef("slow", "1")

	// Activate the actor first.
	if err := h.Send(ref, "warmup"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)

	// Start a request that will block.
	var reqErr error
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, reqErr = h.Request(ref, "blocked")
	}()

	// Give the request time to start.
	time.Sleep(50 * time.Millisecond)

	// Freeze should unblock the in-flight request.
	h.Freeze()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("request was not unblocked by freeze")
	}

	if reqErr == nil {
		t.Fatal("expected error from frozen request")
	}
	t.Logf("in-flight request got error: %v", reqErr)
}

func TestFreeze_ForceStopBadActors(t *testing.T) {
	h := NewHost(
		WithFreezeGracePeriod(200*time.Millisecond),
		WithIdleTimeout(30*time.Second),
	)
	h.Start()
	defer h.Stop()

	started := make(chan struct{})
	block := make(chan struct{}) // never closed — actor blocks forever
	h.RegisterActor("stuck", func() Receiver {
		return &stuckReceiver{started: started, block: block}
	})

	ref := NewRef("stuck", "1")

	// Activate the actor.
	if err := h.Send(ref, "start"); err != nil {
		t.Fatal(err)
	}

	// Wait for Initialize to be received.
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for actor to start")
	}

	// Small sleep to let Register complete (races with started close).
	time.Sleep(50 * time.Millisecond)

	// Verify actor exists.
	if h.actors.Count() == 0 {
		t.Fatal("expected at least 1 actor before freeze")
	}

	// Freeze should force-deregister the stuck actor after grace period.
	h.Freeze()

	// After freeze completes, registry should be empty.
	if count := h.actors.Count(); count != 0 {
		t.Fatalf("expected 0 actors after freeze, got %d", count)
	}
}

func TestFreeze_GracePeriodRespected(t *testing.T) {
	grace := 300 * time.Millisecond
	h := NewHost(
		WithFreezeGracePeriod(grace),
		WithIdleTimeout(30*time.Second),
	)
	h.Start()
	defer h.Stop()

	h.RegisterActor("echo", func() Receiver { return &echoReceiver{} })

	ref := NewRef("echo", "1")
	_, err := h.Request(ref, "activate")
	if err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	h.Freeze()
	elapsed := time.Since(start)

	// If actors exited before grace period, freeze should return early.
	// The echo actor should exit quickly when context is cancelled.
	if elapsed > grace+100*time.Millisecond {
		t.Fatalf("freeze took too long: %v (grace=%v)", elapsed, grace)
	}
}

func TestUnfreeze_ResumesActivation(t *testing.T) {
	h := NewHost(
		WithFreezeGracePeriod(100*time.Millisecond),
		WithIdleTimeout(30*time.Second),
	)
	h.Start()
	defer h.Stop()

	h.RegisterActor("echo", func() Receiver { return &echoReceiver{} })

	// Freeze the host.
	h.Freeze()

	ref := NewRef("echo", "1")
	err := h.Send(ref, "should-fail")
	if !errors.Is(err, ErrHostFrozen) {
		t.Fatalf("expected ErrHostFrozen, got %v", err)
	}

	// Unfreeze (no cluster, so lease check is skipped).
	if err := h.Unfreeze(); err != nil {
		t.Fatal(err)
	}

	if h.IsFrozen() {
		t.Fatal("expected not frozen after unfreeze")
	}

	// Should be able to send messages again.
	result, err := h.Request(ref, "after-unfreeze")
	if err != nil {
		t.Fatalf("Request after unfreeze: %v", err)
	}
	if result != "after-unfreeze" {
		t.Fatalf("expected 'after-unfreeze', got %v", result)
	}
}

func TestFreeze_NoMessagesAfterFreeze(t *testing.T) {
	h := NewHost(
		WithFreezeGracePeriod(100*time.Millisecond),
		WithIdleTimeout(30*time.Second),
	)
	h.Start()
	defer h.Stop()

	var msgCount atomic.Int64
	h.RegisterActor("counter", func() Receiver {
		return ReceiverFunc(func(ctx *Context) error {
			switch ctx.Message.(type) {
			case Initialize, Shutdown:
				return nil
			}
			msgCount.Add(1)
			return nil
		})
	})

	ref := NewRef("counter", "1")

	// Send a message to activate the actor.
	if err := h.Send(ref, "warmup"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)

	countBefore := msgCount.Load()

	// Freeze.
	h.Freeze()

	// After freeze, no new messages should be delivered.
	countAfter := msgCount.Load()

	if countAfter > countBefore {
		t.Fatalf("messages were delivered after freeze: before=%d after=%d",
			countBefore, countAfter)
	}
}

func TestFreeze_ActivationFailsFast(t *testing.T) {
	h := NewHost(
		WithFreezeGracePeriod(100*time.Millisecond),
		WithIdleTimeout(30*time.Second),
	)
	h.Start()
	defer h.Stop()

	h.RegisterActor("test", func() Receiver { return &echoReceiver{} })

	h.Freeze()

	ref := NewRef("test", "1")
	_, err := h.activateActor(ref, false)
	if !errors.Is(err, ErrHostFrozen) {
		t.Fatalf("expected ErrHostFrozen from activateActor, got %v", err)
	}
}

func TestFreeze_DoubleFreeze(t *testing.T) {
	h := NewHost(WithFreezeGracePeriod(100 * time.Millisecond))
	h.Start()
	defer h.Stop()

	// First freeze.
	h.Freeze()
	if !h.IsFrozen() {
		t.Fatal("expected frozen after first Freeze()")
	}

	// Second freeze should be a no-op (no panic).
	h.Freeze()
	if !h.IsFrozen() {
		t.Fatal("expected still frozen after second Freeze()")
	}
}

func TestUnfreeze_WhenNotFrozen(t *testing.T) {
	h := NewHost()
	h.Start()
	defer h.Stop()

	// Unfreeze when not frozen should be a no-op.
	err := h.Unfreeze()
	if err != nil {
		t.Fatalf("expected nil from Unfreeze when not frozen, got %v", err)
	}
}

func TestFreeze_StopAfterFreeze(t *testing.T) {
	h := NewHost(WithFreezeGracePeriod(100 * time.Millisecond))
	h.Start()

	h.RegisterActor("test", func() Receiver { return &echoReceiver{} })

	ref := NewRef("test", "1")
	_, err := h.Request(ref, "activate")
	if err != nil {
		t.Fatal(err)
	}

	// Freeze then stop. Should not panic or deadlock.
	h.Freeze()
	h.Stop()
}

func TestFreeze_RequestManagerFailAll(t *testing.T) {
	rm := NewRequestManager()
	ref := NewRef("test", "1")

	r1 := rm.Create(ref)
	r2 := rm.Create(ref)

	rm.FailAll(ErrHostFrozen)

	// Both requests should get error responses.
	select {
	case res := <-r1.Response:
		if !errors.Is(res.Error, ErrHostFrozen) {
			t.Fatalf("r1: expected ErrHostFrozen, got %v", res.Error)
		}
	default:
		t.Fatal("r1: expected response")
	}

	select {
	case res := <-r2.Response:
		if !errors.Is(res.Error, ErrHostFrozen) {
			t.Fatalf("r2: expected ErrHostFrozen, got %v", res.Error)
		}
	default:
		t.Fatal("r2: expected response")
	}

	// Both should be removed from the manager.
	if rm.Get(r1.ID) != nil {
		t.Fatal("r1 should have been removed")
	}
	if rm.Get(r2.ID) != nil {
		t.Fatal("r2 should have been removed")
	}
}

func TestFreeze_RegistryForceDeregisterAll(t *testing.T) {
	am := NewActorManager()

	// Create a few actors manually (no goroutines needed for this test).
	a1 := &Actor{ref: NewRef("test", "1"), inbox: make(chan InboxMessage, 1), shutdown: make(chan bool, 1)}
	a2 := &Actor{ref: NewRef("test", "2"), inbox: make(chan InboxMessage, 1), shutdown: make(chan bool, 1)}
	am.Register(a1)
	am.Register(a2)

	if am.Count() != 2 {
		t.Fatalf("expected 2 actors, got %d", am.Count())
	}

	refs := am.ForceDeregisterAll()
	if len(refs) != 2 {
		t.Fatalf("expected 2 refs, got %d", len(refs))
	}
	if am.Count() != 0 {
		t.Fatalf("expected 0 actors after ForceDeregisterAll, got %d", am.Count())
	}
}
