package theatre

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// collectReceiver records all received messages for inspection.
type collectReceiver struct {
	mu       sync.Mutex
	messages []interface{}
	got      chan struct{} // closed on first message if non-nil
	gotOnce  sync.Once
}

func (r *collectReceiver) Receive(ctx *Context) error {
	switch ctx.Message.(type) {
	case Initialize, Shutdown:
		return nil
	}
	r.mu.Lock()
	r.messages = append(r.messages, ctx.Message)
	r.mu.Unlock()
	if r.got != nil {
		r.gotOnce.Do(func() { close(r.got) })
	}
	return nil
}

func (r *collectReceiver) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.messages)
}

func TestSendAfter_FiresOnce(t *testing.T) {
	recv := &collectReceiver{got: make(chan struct{})}
	h := NewHost()
	h.RegisterActor("timer", func() Receiver { return recv })
	h.Start()
	defer h.Stop()

	ref := NewRef("timer", "t1")
	id, err := h.SendAfter(ref, "ping", 50*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if id == 0 {
		t.Fatal("expected non-zero schedule ID")
	}

	select {
	case <-recv.got:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for scheduled message")
	}

	// Wait a bit to ensure it doesn't fire again.
	time.Sleep(100 * time.Millisecond)
	if n := recv.count(); n != 1 {
		t.Fatalf("expected 1 message, got %d", n)
	}

	if h.metrics.SchedulesFired.Load() != 1 {
		t.Fatalf("expected SchedulesFired=1, got %d", h.metrics.SchedulesFired.Load())
	}
}

func TestSendAfter_InvalidDelay(t *testing.T) {
	h := NewHost()
	ref := NewRef("timer", "t1")

	_, err := h.SendAfter(ref, "ping", 0)
	if err == nil {
		t.Fatal("expected error for zero delay")
	}

	_, err = h.SendAfter(ref, "ping", -time.Second)
	if err == nil {
		t.Fatal("expected error for negative delay")
	}
}

func TestSendAfter_Cancel(t *testing.T) {
	var received atomic.Bool
	h := NewHost()
	h.RegisterActor("timer", func() Receiver {
		return &funcReceiver{fn: func(ctx *Context) error {
			switch ctx.Message.(type) {
			case Initialize, Shutdown:
			default:
				received.Store(true)
			}
			return nil
		}}
	})
	h.Start()
	defer h.Stop()

	ref := NewRef("timer", "t1")
	id, err := h.SendAfter(ref, "ping", 200*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}

	if err := h.CancelSchedule(id); err != nil {
		t.Fatal(err)
	}

	// Double cancel is a no-op (no error in standalone mode).
	if err := h.CancelSchedule(id); err != nil {
		t.Fatal(err)
	}

	time.Sleep(400 * time.Millisecond)
	if received.Load() {
		t.Fatal("message should not have been delivered after cancel")
	}

	if h.metrics.SchedulesCancelled.Load() != 1 {
		t.Fatalf("expected SchedulesCancelled=1, got %d", h.metrics.SchedulesCancelled.Load())
	}
}

func TestSendCron_RecurringFires(t *testing.T) {
	recv := &collectReceiver{}
	h := NewHost()
	h.RegisterActor("cron", func() Receiver { return recv })
	h.Start()
	defer h.Stop()

	ref := NewRef("cron", "c1")
	// Every minute — we can't wait a full minute in a test, so we test
	// the scheduling mechanics instead.
	id, err := h.SendCron(ref, "tick", "* * * * *")
	if err != nil {
		t.Fatal(err)
	}
	if id == 0 {
		t.Fatal("expected non-zero schedule ID")
	}

	// Verify the schedule was added.
	if h.scheduler.count() != 1 {
		t.Fatalf("expected 1 pending schedule, got %d", h.scheduler.count())
	}

	// Cancel it.
	if err := h.CancelSchedule(id); err != nil {
		t.Fatal(err)
	}
	if h.scheduler.count() != 0 {
		t.Fatalf("expected 0 pending schedules after cancel, got %d", h.scheduler.count())
	}
}

func TestSendCron_InvalidExpression(t *testing.T) {
	h := NewHost()
	ref := NewRef("cron", "c1")

	_, err := h.SendCron(ref, "tick", "bad expr")
	if err == nil {
		t.Fatal("expected error for invalid cron expression")
	}
}

func TestScheduler_CancelAll(t *testing.T) {
	h := NewHost()
	h.RegisterActor("timer", func() Receiver { return &collectReceiver{} })
	h.Start()
	defer h.Stop()

	ref := NewRef("timer", "t1")
	for i := 0; i < 5; i++ {
		h.SendAfter(ref, "ping", time.Hour)
	}

	if h.scheduler.count() != 5 {
		t.Fatalf("expected 5 pending schedules, got %d", h.scheduler.count())
	}

	h.scheduler.cancelAll()

	if h.scheduler.count() != 0 {
		t.Fatalf("expected 0 pending schedules after cancelAll, got %d", h.scheduler.count())
	}
}

func TestScheduler_MultipleSendAfter(t *testing.T) {
	recv := &collectReceiver{}
	h := NewHost()
	h.RegisterActor("timer", func() Receiver { return recv })
	h.Start()
	defer h.Stop()

	ref := NewRef("timer", "t1")
	h.SendAfter(ref, "first", 50*time.Millisecond)
	h.SendAfter(ref, "second", 100*time.Millisecond)

	time.Sleep(300 * time.Millisecond)

	if n := recv.count(); n != 2 {
		t.Fatalf("expected 2 messages, got %d", n)
	}
}

func TestScheduler_ContextMethods(t *testing.T) {
	// Verify context wrapper methods work by scheduling from inside a Receive.
	scheduled := make(chan struct{})

	h := NewHost()
	h.RegisterActor("scheduler", func() Receiver {
		return &funcReceiver{fn: func(ctx *Context) error {
			switch ctx.Message.(type) {
			case Initialize, Shutdown:
				return nil
			case string:
				msg := ctx.Message.(string)
				if msg == "setup" {
					id, err := ctx.SendAfter(ctx.ActorRef, "delayed", 50*time.Millisecond)
					if err != nil || id == 0 {
						t.Errorf("SendAfter failed: id=%d err=%v", id, err)
					}
				}
				if msg == "delayed" {
					close(scheduled)
				}
			}
			return nil
		}}
	})
	h.Start()
	defer h.Stop()

	ref := NewRef("scheduler", "s1")
	h.Send(ref, "setup")

	select {
	case <-scheduled:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for delayed message from context")
	}
}

// funcReceiver is a test helper that wraps a function as a Receiver.
type funcReceiver struct {
	fn func(ctx *Context) error
}

func (r *funcReceiver) Receive(ctx *Context) error {
	return r.fn(ctx)
}
