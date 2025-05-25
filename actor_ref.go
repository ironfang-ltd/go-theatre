package theatre

type ActorRef struct {
	Type string
	ID   string
}

func NewRef(t, id string) ActorRef {
	return ActorRef{
		Type: t,
		ID:   id,
	}
}

func (r ActorRef) String() string {
	return r.Type + ":" + r.ID
}
