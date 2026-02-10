package theatre

import (
	"fmt"
	"sort"
	"sync/atomic"
)

const defaultVirtualNodes = 150

// HashRing is a consistent hash ring that maps keys to host IDs.
// Reads are lock-free (atomic pointer load). Writes rebuild the
// ring immutably and swap the pointer.
type HashRing struct {
	state atomic.Pointer[ringState]
}

type ringState struct {
	vnodes  []vnode
	members []string // sorted
}

type vnode struct {
	hash   uint64
	hostID string
}

// NewHashRing returns an empty ring.
func NewHashRing() *HashRing {
	r := &HashRing{}
	r.state.Store(&ringState{})
	return r
}

// Lookup returns the host responsible for key.
// Returns ("", false) if the ring is empty.
func (r *HashRing) Lookup(key string) (string, bool) {
	s := r.state.Load()
	if len(s.vnodes) == 0 {
		return "", false
	}
	h := fnvHash64(key)
	idx := sort.Search(len(s.vnodes), func(i int) bool {
		return s.vnodes[i].hash >= h
	})
	if idx >= len(s.vnodes) {
		idx = 0 // wrap around
	}
	return s.vnodes[idx].hostID, true
}

// Set rebuilds the ring with the given members. Deterministic:
// same member set always produces the same ring regardless of
// input order.
func (r *HashRing) Set(members []string) {
	sorted := make([]string, len(members))
	copy(sorted, members)
	sort.Strings(sorted)

	var vnodes []vnode
	for _, hostID := range sorted {
		for i := 0; i < defaultVirtualNodes; i++ {
			key := fmt.Sprintf("%s#%d", hostID, i)
			vnodes = append(vnodes, vnode{hash: fnvHash64(key), hostID: hostID})
		}
	}
	sort.Slice(vnodes, func(i, j int) bool {
		return vnodes[i].hash < vnodes[j].hash
	})

	r.state.Store(&ringState{vnodes: vnodes, members: sorted})
}

// Members returns the current member list (sorted).
func (r *HashRing) Members() []string {
	s := r.state.Load()
	out := make([]string, len(s.members))
	copy(out, s.members)
	return out
}

// fnvHash64 returns the FNV-1a 64-bit hash of s.
// Inline implementation avoids the allocation from fnv.New64a()
// and the string→[]byte copy.
func fnvHash64(s string) uint64 {
	const offset64 = 14695981039346656037
	const prime64 = 1099511628211
	h := uint64(offset64)
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= prime64
	}
	return h
}
