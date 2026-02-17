package theatre

import (
	"context"
	"time"
)

// Context is passed to Receiver.Receive on each message. It provides methods
// to send messages, make requests, and reply to the current message.
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

// Send delivers a fire-and-forget message to the actor identified by ref.
func (c *Context) Send(ref Ref, body interface{}) error {
	c.host.sendInternal(OutboxMessage{
		RecipientRef: ref,
		Body:         body,
	})
	return nil
}

// Request sends a message to the actor identified by ref and waits for a reply.
func (c *Context) Request(ref Ref, body interface{}) (any, error) {
	return c.host.requestInternal(ref, body)
}

// SendAfter schedules a one-shot message to be sent after the given delay.
func (c *Context) SendAfter(ref Ref, body interface{}, delay time.Duration) (ScheduleID, error) {
	return c.host.SendAfter(ref, body, delay)
}

// SendCron schedules a recurring message using a 5-field cron expression.
func (c *Context) SendCron(ref Ref, body interface{}, cronExpr string) (ScheduleID, error) {
	return c.host.SendCron(ref, body, cronExpr)
}

// CancelSchedule removes a scheduled message.
func (c *Context) CancelSchedule(id ScheduleID) error {
	return c.host.CancelSchedule(id)
}

// Reply sends a response back to the caller of a Request. For fire-and-forget
// messages (no pending request), Reply is a no-op.
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
