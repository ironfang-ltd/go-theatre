package theater

type InboxMessage struct {
	SenderHostRef HostRef
	RecipientRef  Ref
	IsReply       bool
	ReplyID       int64
	Body          interface{}
	Error         error
}

type OutboxMessage struct {
	RecipientHostRef HostRef
	RecipientRef     Ref
	IsReply          bool
	ReplyID          int64
	Body             interface{}
	Error            error
}

type Initialize struct{}
type Shutdown struct{}
