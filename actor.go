package theatre

import (
	"log/slog"
	"runtime/debug"
	"sync/atomic"
	"time"
)

type Receiver interface {
	Receive(ctx *Context) error
}

type ActorStatus int64

const (
	ActorStatusActive ActorStatus = iota
	ActorStatusInactive
)

type Actor struct {
	host        *Host
	ref         ActorRef
	receiver    Receiver
	inbox       chan InboxMessage
	shutdown    chan bool
	lastMessage int64
	status      int64
}

func NewActor(host *Host, ref ActorRef, receiver Receiver) *Actor {
	return &Actor{
		host:     host,
		ref:      ref,
		receiver: receiver,
		inbox:    make(chan InboxMessage),
		shutdown: make(chan bool),
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

	a.inbox <- msg
}

func (a *Actor) Receive() {

	defer (func() {

		slog.Info("actor shutting down", "type", a.ref.Type, "id", a.ref.ID)

		atomic.CompareAndSwapInt64(&a.status, int64(ActorStatusActive), int64(ActorStatusInactive))

		a.shutdown <- true
	})()

	atomic.CompareAndSwapInt64(&a.status, int64(ActorStatusInactive), int64(ActorStatusActive))

	slog.Info("actor started", "type", a.ref.Type, "id", a.ref.ID)

	ctx := Context{
		ActorRef: a.ref,
		host:     a.host,
	}

	for msg := range a.inbox {

		atomic.StoreInt64(&a.lastMessage, time.Now().Unix())

		ctx.SenderHostRef = msg.SenderHostRef
		ctx.Message = msg.Body
		ctx.replyId = msg.ReplyID

		err := a.receive(&ctx)

		if err != nil {
			slog.Error("actor receive error", "type", a.ref.Type, "id", a.ref.ID, "error", err)
			a.replyWithError(msg, err)
			continue
		}

		if _, ok := msg.Body.(Shutdown); ok {
			break
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

func (a *Actor) GetLastMessageTime() time.Time {
	t := time.Unix(atomic.LoadInt64(&a.lastMessage), 0)
	return t
}

func (a *Actor) receive(ctx *Context) (err error) {

	defer (func() {
		if r := recover(); r != nil {
			debug.PrintStack()
			err = r.(error)
		}
	})()

	return a.receiver.Receive(ctx)
}

func (a *Actor) replyWithError(msg InboxMessage, err error) {

	// if there is a reply ID, send an error response
	if msg.ReplyID != 0 {
		a.host.outbox <- OutboxMessage{
			RecipientHostRef: msg.SenderHostRef,
			RecipientRef:     msg.RecipientRef,
			IsReply:          true,
			ReplyID:          msg.ReplyID,
			Error:            err,
		}
	}
}
