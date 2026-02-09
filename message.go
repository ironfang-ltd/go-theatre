package theatre

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

type Initialize struct {
	Reason ActivationReason
}

type Shutdown struct{}
