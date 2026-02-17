package theatre

// InboxMessage is a message delivered to an actor's inbox for processing.
type InboxMessage struct {
	SenderHostRef HostRef
	RecipientRef  Ref
	IsReply       bool
	ReplyID       int64
	Body          interface{}
	Error         error

	// Remote routing: set when a message arrives from another host
	// via transport. Used to route replies back to the sender.
	senderHostID  string
	senderAddress string
}

// OutboxMessage is a message produced by an actor, routed by the host to its destination.
type OutboxMessage struct {
	RecipientHostRef HostRef
	RecipientRef     Ref
	IsReply          bool
	ReplyID          int64
	Body             interface{}
	Error            error

	// Remote routing: set by ctx.Reply when the original message
	// came from a remote host. processOutbox uses these to route
	// the reply via transport.
	recipientHostID  string
	recipientAddress string
}

// ActivationReason describes why an actor is being activated.
type ActivationReason int

const (
	// ActivationNew means no previous ownership existed.
	ActivationNew ActivationReason = iota
	// ActivationReactivation means this host previously owned the actor.
	ActivationReactivation
	// ActivationFailover means a different host previously owned the actor
	// but its lease expired or epoch changed.
	ActivationFailover
)

// Initialize is the first message delivered to every new actor. Receivers
// can type-switch on it to perform setup work like loading state.
type Initialize struct {
	Reason ActivationReason
}

// Shutdown is sent to an actor when it is being stopped. Receivers can
// type-switch on it to perform cleanup before the actor exits.
type Shutdown struct{}
