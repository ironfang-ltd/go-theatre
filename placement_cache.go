package theatre

import (
	"sync"
)

const placementShards = 64

// PlacementEntry records where an actor is believed to be running.
type PlacementEntry struct {
	HostID   string
	Address  string
	Epoch    int64
	cachedAt int64 // coarse clock seconds (internal)
}

type placementShard struct {
	mu sync.RWMutex
	m  map[Ref]PlacementEntry
}

// PlacementCache maps actor refs to their last-known host location.
// Thread-safe. Entries expire after the configured TTL. Uses 64 shards
// for high-concurrency reads (same pattern as ActorRegistry).
type PlacementCache struct {
	shards [placementShards]placementShard
	ttl    int64 // seconds, compared against coarseNow
}

func newPlacementCache(ttl int64) *PlacementCache {
	pc := &PlacementCache{ttl: ttl}
	for i := range pc.shards {
		pc.shards[i].m = make(map[Ref]PlacementEntry)
	}
	return pc
}

// Get returns the cached placement for ref, or false if missing/expired.
func (pc *PlacementCache) Get(ref Ref) (PlacementEntry, bool) {
	s := &pc.shards[refShard(ref)]
	s.mu.RLock()
	e, ok := s.m[ref]
	s.mu.RUnlock()
	if !ok {
		return PlacementEntry{}, false
	}
	if coarseNow.Load()-e.cachedAt > pc.ttl {
		pc.Evict(ref)
		return PlacementEntry{}, false
	}
	return e, true
}

// Put stores a placement entry for ref.
func (pc *PlacementCache) Put(ref Ref, entry PlacementEntry) {
	entry.cachedAt = coarseNow.Load()
	s := &pc.shards[refShard(ref)]
	s.mu.Lock()
	s.m[ref] = entry
	s.mu.Unlock()
}

// Evict removes the placement entry for ref.
func (pc *PlacementCache) Evict(ref Ref) {
	s := &pc.shards[refShard(ref)]
	s.mu.Lock()
	delete(s.m, ref)
	s.mu.Unlock()
}

// Len returns the number of entries in the cache (including potentially expired ones).
func (pc *PlacementCache) Len() int {
	n := 0
	for i := range pc.shards {
		s := &pc.shards[i]
		s.mu.RLock()
		n += len(s.m)
		s.mu.RUnlock()
	}
	return n
}
