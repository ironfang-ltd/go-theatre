package theatre

type Context struct {
	// The ID of the current actor
	ActorRef Ref

	// The sender host reference
	SenderHostRef HostRef

	// The message being processed
	Message interface{}

	replyId int64
	outbox  chan OutboxMessage
}

func (c *Context) Send(ref Ref, body interface{}) error {
	c.outbox <- OutboxMessage{
		RecipientRef: ref,
		IsReply:      false,
		ReplyID:      0,
		Body:         body,
	}

	return nil
}

func (c *Context) Reply(body interface{}) error {

	c.outbox <- OutboxMessage{
		RecipientHostRef: c.SenderHostRef,
		RecipientRef:     Ref{},
		IsReply:          true,
		ReplyID:          c.replyId,
		Body:             body,
		Error:            nil,
	}

	return nil
}
