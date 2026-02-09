package theatre

import (
	"log/slog"
	"sync"
	"time"
)

const actorShards = 64

type actorShard struct {
	mu sync.RWMutex
	m  map[Ref]*Actor
}

type ActorRegistry struct {
	shards [actorShards]actorShard
}

func NewActorManager() *ActorRegistry {
	am := &ActorRegistry{}
	for i := range am.shards {
		am.shards[i].m = make(map[Ref]*Actor)
	}
	return am
}

// refShard returns the shard index for a Ref using inline FNV-1a.
func refShard(ref Ref) uint32 {
	h := uint32(2166136261)
	for i := 0; i < len(ref.Type); i++ {
		h ^= uint32(ref.Type[i])
		h *= 16777619
	}
	for i := 0; i < len(ref.ID); i++ {
		h ^= uint32(ref.ID[i])
		h *= 16777619
	}
	return h & (actorShards - 1)
}

func (am *ActorRegistry) Register(a *Actor) {
	s := &am.shards[refShard(a.ref)]
	s.mu.Lock()
	s.m[a.ref] = a
	s.mu.Unlock()
}

func (am *ActorRegistry) Lookup(ref Ref) *Actor {
	s := &am.shards[refShard(ref)]
	s.mu.RLock()
	a := s.m[ref]
	s.mu.RUnlock()
	return a
}

func (am *ActorRegistry) Remove(ref Ref) {
	s := &am.shards[refShard(ref)]
	s.mu.Lock()
	a, ok := s.m[ref]
	if ok {
		delete(s.m, ref)
	}
	s.mu.Unlock()
	if ok {
		a.Shutdown()
	}
}

func (am *ActorRegistry) DeregisterOnly(ref Ref) {
	s := &am.shards[refShard(ref)]
	s.mu.Lock()
	delete(s.m, ref)
	s.mu.Unlock()
}

func (am *ActorRegistry) RemoveIdle(idleTimeout time.Duration) {
	for i := range am.shards {
		s := &am.shards[i]
		s.mu.Lock()
		for ref, a := range s.m {
			if time.Since(a.GetLastMessageTime()) > idleTimeout {
				slog.Info("actor idle, shutting down", "type", ref.Type, "id", ref.ID)
				delete(s.m, ref)
				a.Shutdown()
			}
		}
		s.mu.Unlock()
	}
}

func (am *ActorRegistry) RemoveAll() {
	for i := range am.shards {
		s := &am.shards[i]
		s.mu.Lock()
		for ref, a := range s.m {
			a.Shutdown()
			delete(s.m, ref)
		}
		s.mu.Unlock()
	}
}

// ForceDeregisterAll removes all actors from the registry without sending
// Shutdown. Returns the refs of all removed actors so the caller can
// release ownership for each.
func (am *ActorRegistry) ForceDeregisterAll() []Ref {
	var refs []Ref
	for i := range am.shards {
		s := &am.shards[i]
		s.mu.Lock()
		for ref := range s.m {
			refs = append(refs, ref)
			delete(s.m, ref)
		}
		s.mu.Unlock()
	}
	return refs
}

// Count returns the number of registered actors.
func (am *ActorRegistry) Count() int {
	count := 0
	for i := range am.shards {
		s := &am.shards[i]
		s.mu.RLock()
		count += len(s.m)
		s.mu.RUnlock()
	}
	return count
}
