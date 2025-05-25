package theatre

import (
	"fmt"
	"hash/fnv"
	"sync"
)

type OwnedRange struct {
	RangeMin int64
	RangeMax int64
}

type ClusterDirectory struct {
	hostRef HostRef
	actors  map[ActorRef]HostRef
	hosts   map[HostRef]*OwnedRange
	mu      sync.Mutex
}

func NewClusterDirectory(hostRef HostRef) *ClusterDirectory {
	return &ClusterDirectory{
		hostRef: hostRef,
		actors:  make(map[ActorRef]HostRef),
		hosts:   make(map[HostRef]*OwnedRange),
	}
}

// Register an actor in the directory
func (d *ClusterDirectory) Register(ref ActorRef, hostRef HostRef) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.actors[ref] = hostRef
}

func (d *ClusterDirectory) Unregister(ref ActorRef) {
	d.mu.Lock()
	defer d.mu.Unlock()

	delete(d.actors, ref)
}

func (d *ClusterDirectory) Lookup(ref ActorRef) (HostRef, error) {

	// check the actor cache first
	if hostRef, ok := d.getCachedActorRef(ref); ok {
		return hostRef, nil
	}

	// if there is no cached entry for the actor attempt
	// to register the actor with the owner for
	// this node
	hostRef, ok := d.getHostForActor(ref)
	if !ok {
		return HostRef{}, fmt.Errorf("no owning host found for actor: %s", ref)
	}

	// register the actor for this node with the owning host
	ok, err := d.registerActorOnHost(hostRef, ref)
	if err != nil {
		return HostRef{}, fmt.Errorf("actor registration: %w", err)
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	d.actors[ref] = hostRef

	return hostRef, nil
}

func (d *ClusterDirectory) getCachedActorRef(ref ActorRef) (HostRef, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()

	host, ok := d.actors[ref]
	return host, ok
}

func (d *ClusterDirectory) getHostForActor(ref ActorRef) (HostRef, bool) {
	hash := hashToInt64(ref.String())

	d.mu.Lock()
	defer d.mu.Unlock()

	for hostRef, ownedRange := range d.hosts {
		if ownedRange.RangeMin >= hash && ownedRange.RangeMax < hash {
			return hostRef, true
		}
	}

	return HostRef{}, false
}

func (d *ClusterDirectory) registerActorOnHost(hostRef HostRef, ref ActorRef) (bool, error) {
	return false, fmt.Errorf("not yet implemented")
}

func hashToInt64(key string) int64 {
	h := fnv.New64a()
	h.Write([]byte(key))
	return int64(h.Sum64())
}
