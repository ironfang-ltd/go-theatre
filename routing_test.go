package theatre

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// echoReceiver replies with the received message body.
type echoReceiver struct {
	received []interface{}
	mu       sync.Mutex
}

func (r *echoReceiver) Receive(ctx *Context) error {
	switch ctx.Message.(type) {
	case Initialize, Shutdown:
		return nil
	}
	r.mu.Lock()
	r.received = append(r.received, ctx.Message)
	r.mu.Unlock()
	if ctx.replyId != 0 {
		ctx.Reply(ctx.Message)
	}
	return nil
}

func (r *echoReceiver) messages() []interface{} {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]interface{}, len(r.received))
	copy(out, r.received)
	return out
}

// --- helpers ---

// setupHostPair creates two hosts (A and B) with transport, wired together.
// Cluster fields are NOT set (transport-only routing via placement cache).
// Returns hostA, hostB, transportA, transportB, and a cleanup func.
func setupHostPair(t *testing.T) (hostA, hostB *Host, tA, tB *Transport, cleanup func()) {
	t.Helper()

	hostA = NewHost(
		WithIdleTimeout(30*time.Second),
		WithRequestTimeout(5*time.Second),
	)
	hostB = NewHost(
		WithIdleTimeout(30*time.Second),
		WithRequestTimeout(5*time.Second),
	)

	var err error
	tA, err = NewTransport("host-a", "127.0.0.1:0", hostA.HandleTransportMessage)
	if err != nil {
		t.Fatal(err)
	}
	tB, err = NewTransport("host-b", "127.0.0.1:0", hostB.HandleTransportMessage)
	if err != nil {
		t.Fatal(err)
	}

	tA.Start()
	tB.Start()

	// Create a minimal Cluster stand-in. We need cluster != nil for routing,
	// but we don't need a real DB. We'll use a stubCluster.
	stubA := &stubCluster{hostID: "host-a"}
	stubB := &stubCluster{hostID: "host-b"}

	ringAB := NewHashRing()
	ringAB.Set([]string{"host-a", "host-b"})

	hostA.transport = tA
	hostA.cluster = &Cluster{config: ClusterConfig{HostID: "host-a"}, ring: ringAB}
	hostA.placementCache = newPlacementCache(10 * time.Second)
	hostA.cluster.mu.Lock()
	hostA.cluster.epoch = 1
	hostA.cluster.hosts = []HostInfo{
		{HostID: "host-a", Address: tA.Addr(), Epoch: 1},
		{HostID: "host-b", Address: tB.Addr(), Epoch: 1},
	}
	hostA.cluster.mu.Unlock()
	_ = stubA

	hostB.transport = tB
	hostB.cluster = &Cluster{config: ClusterConfig{HostID: "host-b"}, ring: ringAB}
	hostB.placementCache = newPlacementCache(10 * time.Second)
	hostB.cluster.mu.Lock()
	hostB.cluster.epoch = 1
	hostB.cluster.hosts = []HostInfo{
		{HostID: "host-a", Address: tA.Addr(), Epoch: 1},
		{HostID: "host-b", Address: tB.Addr(), Epoch: 1},
	}
	hostB.cluster.mu.Unlock()
	_ = stubB

	hostA.Start()
	hostB.Start()

	cleanup = func() {
		hostA.Stop()
		hostB.Stop()
		tA.Stop()
		tB.Stop()
	}
	return
}

type stubCluster struct {
	hostID string
}

// --- cross-host tests ---

func TestRouting_CrossHostSend(t *testing.T) {
	hostA, hostB, _, tB, cleanup := setupHostPair(t)
	defer cleanup()

	// Register echo actor on B and spawn it.
	recv := &echoReceiver{}
	hostB.RegisterActor("echo", func() Receiver { return recv })
	ref := NewRef("echo", "1")
	if err := hostB.SpawnLocal(ref); err != nil {
		t.Fatal(err)
	}

	// Populate A's placement cache to point to B.
	hostA.placementCache.Put(ref, PlacementEntry{
		HostID:  "host-b",
		Address: tB.Addr(),
		Epoch:   1,
	})

	// Send from A → should forward to B.
	if err := hostA.Send(ref, "hello"); err != nil {
		t.Fatal(err)
	}

	// Wait for delivery.
	deadline := time.After(2 * time.Second)
	for {
		msgs := recv.messages()
		if len(msgs) > 0 {
			if msgs[0] != "hello" {
				t.Fatalf("expected 'hello', got %v", msgs[0])
			}
			break
		}
		select {
		case <-deadline:
			t.Fatal("timeout waiting for message delivery")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func TestRouting_CrossHostRequest(t *testing.T) {
	hostA, hostB, _, tB, cleanup := setupHostPair(t)
	defer cleanup()

	recv := &echoReceiver{}
	hostB.RegisterActor("echo", func() Receiver { return recv })
	ref := NewRef("echo", "1")
	if err := hostB.SpawnLocal(ref); err != nil {
		t.Fatal(err)
	}

	hostA.placementCache.Put(ref, PlacementEntry{
		HostID:  "host-b",
		Address: tB.Addr(),
		Epoch:   1,
	})

	// Request from A → B → reply back to A.
	result, err := hostA.Request(ref, "ping")
	if err != nil {
		t.Fatal(err)
	}
	if result != "ping" {
		t.Fatalf("expected 'ping', got %v", result)
	}
}

func TestRouting_NotHere_EvictsCache(t *testing.T) {
	hostA, _, _, tB, cleanup := setupHostPair(t)
	defer cleanup()

	// NO actor on B. Cache points to B anyway (stale).
	ref := NewRef("echo", "99")
	hostA.placementCache.Put(ref, PlacementEntry{
		HostID:  "host-b",
		Address: tB.Addr(),
		Epoch:   1,
	})

	// Register echo on A so the descriptor exists (for Send to not error).
	hostA.RegisterActor("echo", func() Receiver { return &echoReceiver{} })

	// Send from A → forwards to B → B sends NotHere → A evicts cache.
	hostA.Send(ref, "test")

	// Wait for NotHere to arrive and evict the cache.
	deadline := time.After(2 * time.Second)
	for {
		_, ok := hostA.placementCache.Get(ref)
		if !ok {
			break // evicted
		}
		select {
		case <-deadline:
			t.Fatal("timeout waiting for cache eviction after NotHere")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func TestRouting_EpochMismatch_EvictsCache(t *testing.T) {
	hostA, _, _, tB, cleanup := setupHostPair(t)
	defer cleanup()

	ref := NewRef("echo", "1")

	// Cache has epoch 5 but liveness shows epoch 1 → mismatch.
	hostA.placementCache.Put(ref, PlacementEntry{
		HostID:  "host-b",
		Address: tB.Addr(),
		Epoch:   5, // stale epoch
	})

	// Register echo locally so Send doesn't fail.
	hostA.RegisterActor("echo", func() Receiver { return &echoReceiver{} })

	// Send should detect epoch mismatch, evict cache, and go to
	// resolveAndForward (which returns dead letter since no DB).
	var deadLetterCount int64
	hostA.config.deadLetterHandler = func(msg InboxMessage) {
		atomic.AddInt64(&deadLetterCount, 1)
	}

	hostA.Send(ref, "test")

	// Wait for dead letter.
	deadline := time.After(2 * time.Second)
	for {
		if atomic.LoadInt64(&deadLetterCount) > 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("timeout waiting for dead letter after epoch mismatch")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	// Cache should be evicted.
	_, ok := hostA.placementCache.Get(ref)
	if ok {
		t.Fatal("expected cache eviction after epoch mismatch")
	}
}

func TestRouting_HostDead_EvictsCache(t *testing.T) {
	hostA, _, _, _, cleanup := setupHostPair(t)
	defer cleanup()

	ref := NewRef("echo", "1")

	// Cache points to "host-c" which is not in the liveness list.
	hostA.placementCache.Put(ref, PlacementEntry{
		HostID:  "host-c",
		Address: "127.0.0.1:9999",
		Epoch:   1,
	})

	hostA.RegisterActor("echo", func() Receiver { return &echoReceiver{} })

	var deadLetterCount int64
	hostA.config.deadLetterHandler = func(msg InboxMessage) {
		atomic.AddInt64(&deadLetterCount, 1)
	}

	hostA.Send(ref, "test")

	deadline := time.After(2 * time.Second)
	for {
		if atomic.LoadInt64(&deadLetterCount) > 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("timeout waiting for dead letter after host dead")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	_, ok := hostA.placementCache.Get(ref)
	if ok {
		t.Fatal("expected cache eviction for dead host")
	}
}

func TestRouting_LocalActorDelivery(t *testing.T) {
	hostA, _, _, _, cleanup := setupHostPair(t)
	defer cleanup()

	// Register and spawn actor locally on A.
	recv := &echoReceiver{}
	hostA.RegisterActor("echo", func() Receiver { return recv })
	ref := NewRef("echo", "1")
	if err := hostA.SpawnLocal(ref); err != nil {
		t.Fatal(err)
	}

	// Send should deliver locally (no transport hop).
	hostA.Send(ref, "local-msg")

	deadline := time.After(2 * time.Second)
	for {
		msgs := recv.messages()
		if len(msgs) > 0 {
			if msgs[0] != "local-msg" {
				t.Fatalf("expected 'local-msg', got %v", msgs[0])
			}
			break
		}
		select {
		case <-deadline:
			t.Fatal("timeout waiting for local delivery")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func TestRouting_SpawnLocal(t *testing.T) {
	h := NewHost()
	h.Start()
	defer h.Stop()

	// SpawnLocal without descriptor should fail.
	err := h.SpawnLocal(NewRef("unknown", "1"))
	if err != ErrUnregisteredActorType {
		t.Fatalf("expected ErrUnregisteredActorType, got %v", err)
	}

	// SpawnLocal with descriptor should succeed.
	h.RegisterActor("echo", func() Receiver { return &echoReceiver{} })
	ref := NewRef("echo", "1")
	if err := h.SpawnLocal(ref); err != nil {
		t.Fatal(err)
	}

	// Double spawn should be a no-op.
	if err := h.SpawnLocal(ref); err != nil {
		t.Fatal(err)
	}
}
