package theatre

type InboxMessage struct {
	SenderHostRef HostRef
	RecipientRef  ActorRef
	IsReply       bool
	ReplyID       int64
	Body          interface{}
	Error         error
}

type OutboxMessage struct {
	RecipientHostRef HostRef
	RecipientRef     ActorRef
	IsReply          bool
	ReplyID          int64
	Body             interface{}
	Error            error
}

type Initialize struct{}
type Shutdown struct{}
