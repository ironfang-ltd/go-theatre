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

// ErrStopActor is returned from Receiver.Receive to signal that the actor
// should stop itself. The host will deregister the actor after it exits.
var ErrStopActor = fmt.Errorf("stop actor")

// Receiver is the primary extension interface. Implement Receive to define
// actor behavior. Each call processes one message sequentially.
type Receiver interface {
	Receive(ctx *Context) error
}

// ActorStatus represents the lifecycle state of an actor.
type ActorStatus int64

const (
	// ActorStatusActive indicates the actor is running and processing messages.
	ActorStatusActive ActorStatus = iota
	// ActorStatusInactive indicates the actor has stopped.
	ActorStatusInactive
)

// Actor is a running actor instance. Each actor runs in its own goroutine,
// processing messages sequentially from its inbox channel.
type Actor struct {
	host            *Host
	ref             Ref
	receiver        Receiver
	inbox           chan InboxMessage
	shutdown        chan bool
	lastMessage     int64
	messagesTotal   int64 // total messages processed (atomic)
	errorsTotal     int64 // total receive errors (atomic)
	createdAt       int64 // unix seconds
	status          int64
	selfDeregister   bool        // true = onStop deregisters from actor registry
	releaseOnStop    bool        // true = onDeactivate releases cluster ownership
	onDeactivateHook func(Ref)   // test-only; nil in production (no alloc)
	noPanicRecovery bool

	// Context derived from the host's freezeCtx. Cancelled when the host
	// enters frozen state so the actor can exit cleanly.
	actorCtx    context.Context
	actorCancel context.CancelFunc
}

func newActor(host *Host, ref Ref, receiver Receiver, parentCtx context.Context, inboxSize int) *Actor {
	actorCtx, actorCancel := context.WithCancel(parentCtx)
	return &Actor{
		host:        host,
		ref:         ref,
		receiver:    receiver,
		inbox:       make(chan InboxMessage, inboxSize),
		shutdown:    make(chan bool, 1),
		createdAt:   coarseNow.Load(),
		actorCtx:    actorCtx,
		actorCancel: actorCancel,
	}
}

// GetStatus returns the actor's current lifecycle status.
func (a *Actor) GetStatus() ActorStatus {
	return ActorStatus(atomic.LoadInt64(&a.status))
}

// Send delivers a message to the actor's inbox. If the actor is inactive
// or its context has been cancelled, the message is dropped.
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

// Receive runs the actor's message processing loop. Called as a goroutine
// by the host; it processes messages sequentially until shutdown or context cancellation.
func (a *Actor) Receive() {

	selfStopped := false

	defer (func() {

		slog.Debug("actor shutting down", "type", a.ref.Type, "id", a.ref.ID)

		atomic.CompareAndSwapInt64(&a.status, int64(ActorStatusActive), int64(ActorStatusInactive))

		if a.releaseOnStop {
			a.host.releaseOwnership(a.ref)
		}
		if a.onDeactivateHook != nil {
			a.onDeactivateHook(a.ref)
		}

		if selfStopped && a.selfDeregister {
			a.host.actors.DeregisterOnly(a.ref)
		}

		a.shutdown <- true
	})()

	atomic.CompareAndSwapInt64(&a.status, int64(ActorStatusInactive), int64(ActorStatusActive))

	slog.Debug("actor started", "type", a.ref.Type, "id", a.ref.ID)

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

			atomic.StoreInt64(&a.lastMessage, coarseNow.Load())
			atomic.AddInt64(&a.messagesTotal, 1)

			ctx.SenderHostRef = msg.SenderHostRef
			ctx.Message = msg.Body
			ctx.replyId = msg.ReplyID
			ctx.senderHostID = msg.senderHostID
			ctx.senderAddress = msg.senderAddress

			var err error
			if a.noPanicRecovery {
				err = a.receiver.Receive(&ctx)
			} else {
				err = a.receive(&ctx)
			}

			if err != nil {
				atomic.AddInt64(&a.errorsTotal, 1)
				if errors.Is(err, ErrStopActor) {
					selfStopped = true
					return
				}
				slog.Error("actor receive error", "type", a.ref.Type, "id", a.ref.ID, "error", err)
				a.host.recordActorError(a.ref, "receive error", err.Error())
				a.replyWithError(msg, err)
			}

			if _, ok := msg.Body.(Shutdown); ok {
				return
			}
		}
	}
}

// Shutdown sends a Shutdown message to the actor and blocks until it exits.
func (a *Actor) Shutdown() {
	a.Send(InboxMessage{
		RecipientRef: a.ref,
		Body:         Shutdown{},
	})

	<-a.shutdown
}

// ForceStop cancels the actor's context and closes its inbox, causing
// immediate exit even if the actor is stuck in a blocking operation.
func (a *Actor) ForceStop() {
	a.actorCancel()
	close(a.inbox)
}

// GetLastMessageTime returns the time of the last message processed by the actor.
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
