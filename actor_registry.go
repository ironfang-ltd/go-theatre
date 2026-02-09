package theatre

import (
	"log/slog"
	"sync"
	"time"
)

type ActorRegistry struct {
	actors map[Ref]*Actor
	mu     sync.RWMutex
}

func NewActorManager() *ActorRegistry {
	return &ActorRegistry{
		actors: make(map[Ref]*Actor),
	}
}

func (am *ActorRegistry) Register(a *Actor) {
	am.mu.Lock()
	defer am.mu.Unlock()

	am.actors[a.ref] = a
}

func (am *ActorRegistry) Lookup(ref Ref) *Actor {
	am.mu.RLock()
	defer am.mu.RUnlock()

	return am.actors[ref]
}

func (am *ActorRegistry) Remove(ref Ref) {
	am.mu.Lock()
	defer am.mu.Unlock()

	a := am.actors[ref]
	if a == nil {
		return
	}

	delete(am.actors, ref)

	a.Shutdown()
}

func (am *ActorRegistry) DeregisterOnly(ref Ref) {
	am.mu.Lock()
	defer am.mu.Unlock()

	delete(am.actors, ref)
}

func (am *ActorRegistry) RemoveIdle(idleTimeout time.Duration) {
	am.mu.Lock()
	defer am.mu.Unlock()

	for ref, a := range am.actors {
		if time.Since(a.GetLastMessageTime()) > idleTimeout {
			slog.Info("actor idle, shutting down", "type", ref.Type, "id", ref.ID)
			delete(am.actors, ref)
			a.Shutdown()
		}
	}
}

func (am *ActorRegistry) RemoveAll() {
	am.mu.Lock()
	defer am.mu.Unlock()

	for ref, a := range am.actors {
		a.Shutdown()
		delete(am.actors, ref)
	}
}

// ForceDeregisterAll removes all actors from the registry without sending
// Shutdown. Returns the refs of all removed actors so the caller can
// release ownership for each.
func (am *ActorRegistry) ForceDeregisterAll() []Ref {
	am.mu.Lock()
	defer am.mu.Unlock()

	refs := make([]Ref, 0, len(am.actors))
	for ref := range am.actors {
		refs = append(refs, ref)
	}
	// Clear the map.
	am.actors = make(map[Ref]*Actor)
	return refs
}

// Count returns the number of registered actors.
func (am *ActorRegistry) Count() int {
	am.mu.RLock()
	defer am.mu.RUnlock()
	return len(am.actors)
}
