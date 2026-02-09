package theatre

import (
	"fmt"
	"testing"
)

func TestHashRing_EmptyRing(t *testing.T) {
	r := NewHashRing()
	_, ok := r.Lookup("anything")
	if ok {
		t.Fatal("expected empty ring to return false")
	}
}

func TestHashRing_SingleHost(t *testing.T) {
	r := NewHashRing()
	r.Set([]string{"host-a"})

	for i := 0; i < 100; i++ {
		host, ok := r.Lookup(fmt.Sprintf("key-%d", i))
		if !ok {
			t.Fatal("expected lookup to succeed")
		}
		if host != "host-a" {
			t.Fatalf("expected host-a, got %s", host)
		}
	}
}

func TestHashRing_Deterministic(t *testing.T) {
	r1 := NewHashRing()
	r1.Set([]string{"host-c", "host-a", "host-b"}) // unsorted input

	r2 := NewHashRing()
	r2.Set([]string{"host-b", "host-a", "host-c"}) // different order

	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("actor-%d", i)
		h1, _ := r1.Lookup(key)
		h2, _ := r2.Lookup(key)
		if h1 != h2 {
			t.Fatalf("key %q: ring1=%s ring2=%s — not deterministic", key, h1, h2)
		}
	}
}

func TestHashRing_Distribution(t *testing.T) {
	r := NewHashRing()
	hosts := []string{"host-a", "host-b", "host-c"}
	r.Set(hosts)

	counts := make(map[string]int)
	const n = 10_000
	for i := 0; i < n; i++ {
		host, ok := r.Lookup(fmt.Sprintf("key-%d", i))
		if !ok {
			t.Fatal("expected lookup to succeed")
		}
		counts[host]++
	}

	// With 3 hosts and 150 vnodes each, expect roughly 33% per host.
	// Allow 15–50% range to avoid flaky tests.
	for _, h := range hosts {
		pct := float64(counts[h]) / float64(n) * 100
		if pct < 15 || pct > 50 {
			t.Fatalf("host %s got %.1f%% of keys (expected 15–50%%)", h, pct)
		}
		t.Logf("host %s: %d keys (%.1f%%)", h, counts[h], pct)
	}
}

func TestHashRing_MembershipChange(t *testing.T) {
	r := NewHashRing()
	r.Set([]string{"host-a", "host-b", "host-c"})

	// Record assignments with 3 hosts.
	before := make(map[string]string)
	const n = 1000
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%d", i)
		host, _ := r.Lookup(key)
		before[key] = host
	}

	// Remove host-c.
	r.Set([]string{"host-a", "host-b"})

	moved := 0
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%d", i)
		host, _ := r.Lookup(key)
		if host != before[key] {
			moved++
		}
		// Keys that were on host-c must move.
		if before[key] == "host-c" && host == "host-c" {
			t.Fatalf("key %q still on removed host-c", key)
		}
	}

	// Consistent hashing: only ~1/3 of keys should move (those on host-c).
	// Allow up to 50% to avoid flakiness.
	pct := float64(moved) / float64(n) * 100
	if pct > 55 {
		t.Fatalf("%.1f%% of keys moved — too many for consistent hashing", pct)
	}
	t.Logf("%d/%d keys moved (%.1f%%)", moved, n, pct)
}

func TestHashRing_Members(t *testing.T) {
	r := NewHashRing()
	if len(r.Members()) != 0 {
		t.Fatal("expected empty members")
	}

	r.Set([]string{"host-b", "host-a"})
	m := r.Members()
	if len(m) != 2 || m[0] != "host-a" || m[1] != "host-b" {
		t.Fatalf("expected sorted [host-a host-b], got %v", m)
	}
}
