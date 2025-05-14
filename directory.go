package theater

import "sync"

type Directory interface {
	Register(ref Ref, hostRef HostRef)
	Lookup(ref Ref) (HostRef, bool)
	Unregister(ref Ref)
}

type directory struct {
	actors map[Ref]HostRef
	mu     sync.Mutex
}

func NewDirectory() Directory {
	return &directory{
		actors: make(map[Ref]HostRef),
	}
}

func (d *directory) Register(ref Ref, hostRef HostRef) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.actors[ref] = hostRef
}

func (d *directory) Lookup(ref Ref) (HostRef, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()

	hostRef, ok := d.actors[ref]
	return hostRef, ok
}

func (d *directory) Unregister(ref Ref) {
	d.mu.Lock()
	defer d.mu.Unlock()

	delete(d.actors, ref)
}
