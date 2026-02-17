package theatre

// Ref identifies an actor by its type and unique ID within that type.
type Ref struct {
	Type string
	ID   string
}

// NewRef creates a new actor reference with the given type and ID.
func NewRef(t, id string) Ref {
	return Ref{
		Type: t,
		ID:   id,
	}
}

// String returns the actor reference in "Type:ID" format.
func (r Ref) String() string {
	return r.Type + ":" + r.ID
}
