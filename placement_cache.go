package theatre

import (
	"sync"
	"time"
)

// PlacementEntry records where an actor is believed to be running.
type PlacementEntry struct {
	HostID   string
	Address  string
	Epoch    int64
	CachedAt time.Time
}

// PlacementCache maps actor refs to their last-known host location.
// Thread-safe. Entries expire after the configured TTL.
type PlacementCache struct {
	mu      sync.RWMutex
	entries map[Ref]PlacementEntry
	ttl     time.Duration
}

func newPlacementCache(ttl time.Duration) *PlacementCache {
	return &PlacementCache{
		entries: make(map[Ref]PlacementEntry),
		ttl:     ttl,
	}
}

// Get returns the cached placement for ref, or false if missing/expired.
func (pc *PlacementCache) Get(ref Ref) (PlacementEntry, bool) {
	pc.mu.RLock()
	e, ok := pc.entries[ref]
	pc.mu.RUnlock()
	if !ok {
		return PlacementEntry{}, false
	}
	if time.Since(e.CachedAt) > pc.ttl {
		pc.Evict(ref)
		return PlacementEntry{}, false
	}
	return e, true
}

// Put stores a placement entry for ref.
func (pc *PlacementCache) Put(ref Ref, entry PlacementEntry) {
	entry.CachedAt = time.Now()
	pc.mu.Lock()
	pc.entries[ref] = entry
	pc.mu.Unlock()
}

// Evict removes the placement entry for ref.
func (pc *PlacementCache) Evict(ref Ref) {
	pc.mu.Lock()
	delete(pc.entries, ref)
	pc.mu.Unlock()
}

// Len returns the number of entries in the cache (including potentially expired ones).
func (pc *PlacementCache) Len() int {
	pc.mu.RLock()
	n := len(pc.entries)
	pc.mu.RUnlock()
	return n
}
