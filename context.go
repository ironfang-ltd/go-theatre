package theatre

type Context struct {
	// The ID of the current actor
	ActorRef Ref

	// The sender host reference
	SenderHostRef HostRef

	// The message being processed
	Message interface{}

	host    *Host
	replyId int64
}

func (c *Context) Send(ref Ref, body interface{}) error {
	c.host.sendInternal(OutboxMessage{
		RecipientRef: ref,
		Body:         body,
	})
	return nil
}

func (c *Context) Request(ref Ref, body interface{}) (any, error) {
	return c.host.requestInternal(ref, body)
}

func (c *Context) Reply(body interface{}) error {

	c.host.sendInternal(OutboxMessage{
		RecipientHostRef: c.SenderHostRef,
		RecipientRef:     Ref{},
		IsReply:          true,
		ReplyID:          c.replyId,
		Body:             body,
		Error:            nil,
	})

	return nil
}
