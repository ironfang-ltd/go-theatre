package theatre

import (
	"testing"
	"time"
)

func TestPlacementCache_PutGet(t *testing.T) {
	pc := newPlacementCache(10 * time.Second)
	ref := NewRef("player", "1")

	_, ok := pc.Get(ref)
	if ok {
		t.Fatal("expected miss on empty cache")
	}

	pc.Put(ref, PlacementEntry{HostID: "host-a", Address: "127.0.0.1:7000", Epoch: 1})
	e, ok := pc.Get(ref)
	if !ok {
		t.Fatal("expected hit after put")
	}
	if e.HostID != "host-a" || e.Epoch != 1 {
		t.Fatalf("unexpected entry: %+v", e)
	}
}

func TestPlacementCache_TTLExpiry(t *testing.T) {
	pc := newPlacementCache(50 * time.Millisecond)
	ref := NewRef("player", "2")

	pc.Put(ref, PlacementEntry{HostID: "host-b", Address: "127.0.0.1:7001", Epoch: 1})

	_, ok := pc.Get(ref)
	if !ok {
		t.Fatal("expected hit before TTL")
	}

	time.Sleep(60 * time.Millisecond)

	_, ok = pc.Get(ref)
	if ok {
		t.Fatal("expected miss after TTL expiry")
	}
}

func TestPlacementCache_Evict(t *testing.T) {
	pc := newPlacementCache(10 * time.Second)
	ref := NewRef("player", "3")

	pc.Put(ref, PlacementEntry{HostID: "host-c", Address: "127.0.0.1:7002", Epoch: 2})

	pc.Evict(ref)

	_, ok := pc.Get(ref)
	if ok {
		t.Fatal("expected miss after evict")
	}
}

func TestPlacementCache_EpochOverwrite(t *testing.T) {
	pc := newPlacementCache(10 * time.Second)
	ref := NewRef("player", "4")

	pc.Put(ref, PlacementEntry{HostID: "host-a", Address: "127.0.0.1:7000", Epoch: 1})
	pc.Put(ref, PlacementEntry{HostID: "host-b", Address: "127.0.0.1:7001", Epoch: 2})

	e, ok := pc.Get(ref)
	if !ok {
		t.Fatal("expected hit")
	}
	if e.HostID != "host-b" || e.Epoch != 2 {
		t.Fatalf("expected updated entry, got %+v", e)
	}
}
