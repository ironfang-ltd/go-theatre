package theatre

type Directory interface {
	Register(ref ActorRef, hostRef HostRef)
	Lookup(ref ActorRef) (HostRef, error)
	Unregister(ref ActorRef)
}

type directory struct {
	hostRef HostRef
}

func NewDirectory(hostRef HostRef) Directory {
	return &directory{
		hostRef: hostRef,
	}
}

func (d *directory) Register(_ ActorRef, _ HostRef) {
}

func (d *directory) Lookup(_ ActorRef) (HostRef, error) {
	return d.hostRef, nil
}

func (d *directory) Unregister(_ ActorRef) {
}
