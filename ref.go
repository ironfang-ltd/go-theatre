package theater

type Ref struct {
	Type string
	ID   string
}

func NewRef(t, id string) Ref {
	return Ref{
		Type: t,
		ID:   id,
	}
}

func (r Ref) String() string {
	return r.Type + ":" + r.ID
}
