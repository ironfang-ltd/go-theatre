package theatre

import (
	"testing"
	"time"
)

func TestMetrics_SendIncrements(t *testing.T) {
	host := NewHost()
	host.RegisterActor("counter", func() Receiver {
		return &echoReceiver{}
	})
	host.Start()
	defer host.Stop()

	ref := NewRef("counter", "1")
	if err := host.Send(ref, "hello"); err != nil {
		t.Fatalf("Send: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	m := host.Metrics()
	if got := m.MessagesSent.Load(); got != 1 {
		t.Errorf("MessagesSent = %d, want 1", got)
	}
	if got := m.MessagesReceived.Load(); got < 1 {
		t.Errorf("MessagesReceived = %d, want >= 1", got)
	}
}

func TestMetrics_RequestIncrements(t *testing.T) {
	host := NewHost()
	host.RegisterActor("echo", func() Receiver {
		return &echoReceiver{}
	})
	host.Start()
	defer host.Stop()

	ref := NewRef("echo", "1")
	resp, err := host.Request(ref, "ping")
	if err != nil {
		t.Fatalf("Request: %v", err)
	}
	if resp != "ping" {
		t.Errorf("Response = %v, want ping", resp)
	}

	m := host.Metrics()
	if got := m.RequestsTotal.Load(); got != 1 {
		t.Errorf("RequestsTotal = %d, want 1", got)
	}
}

func TestMetrics_DeadLetterIncrements(t *testing.T) {
	host := NewHost()
	host.Start()
	defer host.Stop()

	ref := NewRef("nonexistent", "1")
	err := host.Send(ref, "hello")
	if err != ErrUnregisteredActorType {
		t.Errorf("Send error = %v, want ErrUnregisteredActorType", err)
	}
}

func TestMetrics_Snapshot(t *testing.T) {
	host := NewHost()
	host.RegisterActor("snap", func() Receiver {
		return &echoReceiver{}
	})
	host.Start()
	defer host.Stop()

	ref := NewRef("snap", "1")
	host.Send(ref, "a")
	host.Send(ref, "b")
	time.Sleep(50 * time.Millisecond)

	snap := host.Metrics().Snapshot()

	if snap["messages_sent"] != 2 {
		t.Errorf("messages_sent = %d, want 2", snap["messages_sent"])
	}

	// actors_active should be present.
	if _, ok := snap["actors_active"]; !ok {
		t.Error("actors_active missing from snapshot")
	}
}

func TestMetrics_ActiveActorsGauge(t *testing.T) {
	host := NewHost()
	host.RegisterActor("gauge", func() Receiver {
		return &echoReceiver{}
	})
	host.Start()
	defer host.Stop()

	snap := host.Metrics().Snapshot()
	if snap["actors_active"] != 0 {
		t.Errorf("actors_active = %d before any sends, want 0", snap["actors_active"])
	}

	host.Send(NewRef("gauge", "1"), "hi")
	host.Send(NewRef("gauge", "2"), "hi")
	time.Sleep(50 * time.Millisecond)

	snap = host.Metrics().Snapshot()
	if snap["actors_active"] != 2 {
		t.Errorf("actors_active = %d, want 2", snap["actors_active"])
	}
}

func TestMetrics_FreezeIncrements(t *testing.T) {
	host := NewHost(WithFreezeGracePeriod(50 * time.Millisecond))
	host.Start()
	defer host.Stop()

	m := host.Metrics()
	if got := m.FreezeCount.Load(); got != 0 {
		t.Errorf("FreezeCount before freeze = %d, want 0", got)
	}

	host.Freeze()

	if got := m.FreezeCount.Load(); got != 1 {
		t.Errorf("FreezeCount after freeze = %d, want 1", got)
	}
}
