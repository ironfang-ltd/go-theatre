package theatre

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync/atomic"
	"time"
)

var ErrStopActor = fmt.Errorf("stop actor")

type Receiver interface {
	Receive(ctx *Context) error
}

type ActorStatus int64

const (
	ActorStatusActive ActorStatus = iota
	ActorStatusInactive
)

type Actor struct {
	host         *Host
	ref          Ref
	receiver     Receiver
	inbox        chan InboxMessage
	shutdown     chan bool
	lastMessage  int64
	status       int64
	onStop       func(Ref)
	onDeactivate func(Ref)

	// Context derived from the host's freezeCtx. Cancelled when the host
	// enters frozen state so the actor can exit cleanly.
	actorCtx    context.Context
	actorCancel context.CancelFunc
}

func NewActor(host *Host, ref Ref, receiver Receiver, parentCtx context.Context, inboxSize int) *Actor {
	actorCtx, actorCancel := context.WithCancel(parentCtx)
	return &Actor{
		host:        host,
		ref:         ref,
		receiver:    receiver,
		inbox:       make(chan InboxMessage, inboxSize),
		shutdown:    make(chan bool, 1),
		actorCtx:    actorCtx,
		actorCancel: actorCancel,
	}
}

func (a *Actor) GetStatus() ActorStatus {
	return ActorStatus(atomic.LoadInt64(&a.status))
}

func (a *Actor) Send(msg InboxMessage) {

	if a.GetStatus() == ActorStatusInactive {
		slog.Error("actor not active", "type", a.ref.Type, "id", a.ref.ID)
		return
	}

	select {
	case a.inbox <- msg:
	case <-a.actorCtx.Done():
		// Actor is being cancelled (host freeze). Don't block.
	}
}

func (a *Actor) Receive() {

	selfStopped := false

	defer (func() {

		slog.Info("actor shutting down", "type", a.ref.Type, "id", a.ref.ID)

		atomic.CompareAndSwapInt64(&a.status, int64(ActorStatusActive), int64(ActorStatusInactive))

		if a.onDeactivate != nil {
			a.onDeactivate(a.ref)
		}

		if selfStopped && a.onStop != nil {
			a.onStop(a.ref)
		}

		a.shutdown <- true
	})()

	atomic.CompareAndSwapInt64(&a.status, int64(ActorStatusInactive), int64(ActorStatusActive))

	slog.Info("actor started", "type", a.ref.Type, "id", a.ref.ID)

	ctx := Context{
		ActorRef: a.ref,
		host:     a.host,
		Ctx:      a.actorCtx,
	}

	for {
		// Priority check: bail out if context cancelled (host frozen).
		select {
		case <-a.actorCtx.Done():
			return
		default:
		}

		// Wait for next message or cancellation.
		select {
		case <-a.actorCtx.Done():
			return
		case msg, ok := <-a.inbox:
			if !ok {
				// Inbox closed (force-stop).
				return
			}

			atomic.StoreInt64(&a.lastMessage, time.Now().Unix())

			ctx.SenderHostRef = msg.SenderHostRef
			ctx.Message = msg.Body
			ctx.replyId = msg.ReplyID
			ctx.senderHostID = msg.senderHostID
			ctx.senderAddress = msg.senderAddress

			err := a.receive(&ctx)

			if err != nil {
				if errors.Is(err, ErrStopActor) {
					selfStopped = true
					return
				}
				slog.Error("actor receive error", "type", a.ref.Type, "id", a.ref.ID, "error", err)
				a.replyWithError(msg, err)
			}

			if _, ok := msg.Body.(Shutdown); ok {
				return
			}
		}
	}
}

func (a *Actor) Shutdown() {
	a.Send(InboxMessage{
		RecipientRef: a.ref,
		Body:         Shutdown{},
	})

	<-a.shutdown
}

func (a *Actor) ForceStop() {
	a.actorCancel()
	close(a.inbox)
}

func (a *Actor) GetLastMessageTime() time.Time {
	t := time.Unix(atomic.LoadInt64(&a.lastMessage), 0)
	return t
}

func (a *Actor) receive(ctx *Context) (err error) {

	defer (func() {
		if r := recover(); r != nil {
			debug.PrintStack()
			if e, ok := r.(error); ok {
				err = e
			} else {
				err = fmt.Errorf("panic: %v", r)
			}
		}
	})()

	return a.receiver.Receive(ctx)
}

func (a *Actor) replyWithError(msg InboxMessage, err error) {

	// if there is a reply ID, send an error response
	if msg.ReplyID != 0 {
		a.host.sendInternal(OutboxMessage{
			RecipientHostRef: msg.SenderHostRef,
			RecipientRef:     msg.RecipientRef,
			IsReply:          true,
			ReplyID:          msg.ReplyID,
			Error:            err,
			recipientHostID:  msg.senderHostID,
			recipientAddress: msg.senderAddress,
		})
	}
}
