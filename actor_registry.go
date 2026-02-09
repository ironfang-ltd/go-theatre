package theatre

import (
	"log/slog"
	"sync"
	"time"
)

type ActorRegistry struct {
	actors sync.Map // map[Ref]*Actor
}

func NewActorManager() *ActorRegistry {
	return &ActorRegistry{}
}

func (am *ActorRegistry) Register(a *Actor) {
	am.actors.Store(a.ref, a)
}

func (am *ActorRegistry) Lookup(ref Ref) *Actor {
	v, ok := am.actors.Load(ref)
	if !ok {
		return nil
	}
	return v.(*Actor)
}

func (am *ActorRegistry) Remove(ref Ref) {
	v, loaded := am.actors.LoadAndDelete(ref)
	if !loaded {
		return
	}
	v.(*Actor).Shutdown()
}

func (am *ActorRegistry) DeregisterOnly(ref Ref) {
	am.actors.Delete(ref)
}

func (am *ActorRegistry) RemoveIdle(idleTimeout time.Duration) {
	am.actors.Range(func(key, value any) bool {
		ref := key.(Ref)
		a := value.(*Actor)
		if time.Since(a.GetLastMessageTime()) > idleTimeout {
			slog.Info("actor idle, shutting down", "type", ref.Type, "id", ref.ID)
			am.actors.Delete(ref)
			a.Shutdown()
		}
		return true
	})
}

func (am *ActorRegistry) RemoveAll() {
	am.actors.Range(func(key, value any) bool {
		a := value.(*Actor)
		a.Shutdown()
		am.actors.Delete(key)
		return true
	})
}

// ForceDeregisterAll removes all actors from the registry without sending
// Shutdown. Returns the refs of all removed actors so the caller can
// release ownership for each.
func (am *ActorRegistry) ForceDeregisterAll() []Ref {
	var refs []Ref
	am.actors.Range(func(key, value any) bool {
		refs = append(refs, key.(Ref))
		am.actors.Delete(key)
		return true
	})
	return refs
}

// Count returns the number of registered actors.
func (am *ActorRegistry) Count() int {
	count := 0
	am.actors.Range(func(_, _ any) bool {
		count++
		return true
	})
	return count
}
