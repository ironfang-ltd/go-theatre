package theatre

import "context"

type Context struct {
	// The ID of the current actor
	ActorRef Ref

	// The sender host reference
	SenderHostRef HostRef

	// The message being processed
	Message interface{}

	// Ctx is the actor's context, derived from the host's freeze context.
	// Receivers can check Ctx.Done() to detect freeze cancellation during
	// long-running operations.
	Ctx context.Context

	host    *Host
	replyId int64

	// Remote routing: set when message came from a remote host.
	senderHostID  string
	senderAddress string
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
	if c.replyId == 0 {
		return nil // fire-and-forget message, no reply expected
	}

	c.host.sendInternal(OutboxMessage{
		RecipientHostRef: c.SenderHostRef,
		RecipientRef:     Ref{},
		IsReply:          true,
		ReplyID:          c.replyId,
		Body:             body,
		Error:            nil,
		recipientHostID:  c.senderHostID,
		recipientAddress: c.senderAddress,
	})

	return nil
}
