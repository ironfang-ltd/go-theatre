package theater

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

func (am *ActorRegistry) RemoveIdle() {
	am.mu.Lock()
	defer am.mu.Unlock()

	for ref, a := range am.actors {
		if time.Since(a.GetLastMessageTime()) > 15*time.Second {
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
