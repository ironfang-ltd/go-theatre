package theatre

import (
	"context"
	"time"
)

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
