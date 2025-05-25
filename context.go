package theatre

type Context struct {
	// The ID of the current actor
	ActorRef ActorRef

	// The sender host reference
	SenderHostRef HostRef

	// The message being processed
	Message interface{}

	host    *Host
	replyId int64
}

func (c *Context) Send(ref ActorRef, body interface{}) error {
	return c.host.Send(ref, body)
}

func (c *Context) Request(ref ActorRef, body interface{}) (any, error) {
	return c.host.Request(ref, body)
}

func (c *Context) Reply(body interface{}) error {

	c.host.outbox <- OutboxMessage{
		RecipientHostRef: c.SenderHostRef,
		RecipientRef:     ActorRef{},
		IsReply:          true,
		ReplyID:          c.replyId,
		Body:             body,
		Error:            nil,
	}

	return nil
}
