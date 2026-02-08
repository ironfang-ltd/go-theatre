package theatre

import (
	"fmt"
	"log/slog"
	"sync"
	"time"
)

var (
	ErrUnregisteredActorType = fmt.Errorf("unregistered actor type")
	ErrHostDraining          = fmt.Errorf("host is draining")
)

type Creator func() Receiver

type Descriptor struct {
	Name   string
	Create Creator
}

type Response struct {
	Body  interface{}
	Error error
}

type Host struct {
	hostRef     HostRef
	config      hostConfig
	descriptors map[string]*Descriptor
	actors      *ActorRegistry
	requests    *RequestManager
	directory   Directory

	mu      sync.RWMutex
	resPool sync.Pool
	outbox  chan OutboxMessage
	inbox   chan InboxMessage
	drain   chan struct{}
	done    chan struct{}
}

func NewHost(opts ...Option) *Host {

	cfg := defaultHostConfig()
	for _, o := range opts {
		o(&cfg)
	}

	hostRef, err := createNewHostRef()
	if err != nil {
		panic(err)
	}

	return &Host{
		hostRef:     hostRef,
		config:      cfg,
		mu:          sync.RWMutex{},
		descriptors: make(map[string]*Descriptor),
		requests:    NewRequestManager(),
		directory:   NewDirectory(),
		actors:      NewActorManager(),
		resPool: sync.Pool{
			New: func() interface{} {
				return &Response{}
			},
		},
		outbox: make(chan OutboxMessage, 512),
		inbox:  make(chan InboxMessage, 512),
		drain:  make(chan struct{}),
		done:   make(chan struct{}),
	}
}

func (m *Host) Start() {

	slog.Info("starting", "host", m.hostRef.String())

	go m.cleanup()
	go m.processOutbox()
	go m.processInbox()
}

func (m *Host) Stop() {

	slog.Info("stopping", "host", m.hostRef.String())

	// phase 1: close drain to reject new external messages
	close(m.drain)

	// wait for in-flight messages to be processed or timeout
	m.waitForDrain()

	// phase 2: close done to stop processing goroutines
	close(m.done)
	m.actors.RemoveAll()
}

func (m *Host) sendInternal(msg OutboxMessage) {
	m.outbox <- msg
}

func (m *Host) waitForDrain() {
	deadline := time.After(m.config.drainTimeout)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-deadline:
			slog.Warn("drain timeout reached", "host", m.hostRef.String())
			return
		case <-ticker.C:
			if len(m.outbox) == 0 && len(m.inbox) == 0 {
				return
			}
		}
	}
}

func (m *Host) RegisterActor(name string, creator Creator) {

	m.mu.Lock()
	defer m.mu.Unlock()

	m.descriptors[name] = &Descriptor{
		Name:   name,
		Create: creator,
	}
}

func (m *Host) hasDescriptor(typeName string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.descriptors[typeName]
	return ok
}

func (m *Host) Send(ref Ref, body interface{}) error {

	select {
	case <-m.drain:
		return ErrHostDraining
	default:
	}

	if !m.hasDescriptor(ref.Type) {
		return ErrUnregisteredActorType
	}

	m.outbox <- OutboxMessage{
		RecipientRef: ref,
		Body:         body,
	}

	return nil
}

func (m *Host) Request(ref Ref, body interface{}) (interface{}, error) {

	select {
	case <-m.drain:
		return nil, ErrHostDraining
	default:
	}

	if !m.hasDescriptor(ref.Type) {
		return nil, ErrUnregisteredActorType
	}

	return m.requestInternal(ref, body)
}

func (m *Host) requestInternal(ref Ref, body interface{}) (interface{}, error) {

	// create a new request to track the response
	req := m.requests.Create(ref)

	// send the request to the actor
	m.outbox <- OutboxMessage{
		RecipientRef: ref,
		Body:         body,
		ReplyID:      req.ID,
	}

	defer (func() {
		m.requests.Remove(req.ID)
	})()

	res := <-req.Response

	resBody := res.Body
	err := res.Error

	m.resPool.Put(res)

	return resBody, err
}

func (m *Host) processInbox() {
	for {
		select {
		case <-m.done:
			return
		case msg := <-m.inbox:

			if msg.IsReply {

				req := m.requests.Get(msg.ReplyID)
				if req != nil {
					res := m.resPool.Get().(*Response)
					res.Body = msg.Body
					res.Error = msg.Error
					req.Response <- res
				}
				continue
			}

			// try to find the actor in the local registry
			a := m.actors.Lookup(msg.RecipientRef)
			if a == nil {

				// if the actor is not found, create it
				a = m.createLocalActor(msg.RecipientRef)
				if a == nil {
					slog.Error("failed to create actor", "type", msg.RecipientRef.Type, "id", msg.RecipientRef.ID)
					if m.config.deadLetterHandler != nil {
						m.config.deadLetterHandler(msg)
					}
					continue
				}
			}

			a.Send(msg)
		}
	}
}

func (m *Host) processOutbox() {

	// process messages in the outbox queue
	// and forward them to the appropriate actor

	for {
		select {
		case <-m.done:
			return
		case msg := <-m.outbox:

			// check the directory to see if the actor is registered
			/*hostRef, ok := m.directory.Lookup(msg.To)
			if ok {
				if hostRef != m.hostRef {
					// forward the message to the remote host
					slog.Info("forwarding message to remote host", "type", msg.To.Type, "id", msg.To.ID, "host", hostRef.String())
					continue
				}
			}*/

			// route all messages to the local actor manager
			m.inbox <- InboxMessage{
				SenderHostRef: m.hostRef,
				RecipientRef:  msg.RecipientRef,
				Body:          msg.Body,
				ReplyID:       msg.ReplyID,
				IsReply:       msg.IsReply,
				Error:         msg.Error,
			}
		}
	}
}

func (m *Host) createLocalActor(ref Ref) *Actor {

	m.mu.RLock()
	defer m.mu.RUnlock()

	d, ok := m.descriptors[ref.Type]
	if !ok {
		return nil
	}

	receiver := d.Create()

	a := NewActor(m, ref, receiver)
	a.onStop = func(r Ref) {
		m.actors.DeregisterOnly(r)
	}
	go a.Receive()

	a.inbox <- InboxMessage{
		SenderHostRef: m.hostRef,
		RecipientRef:  ref,
		Body:          Initialize{},
	}

	m.actors.Register(a)

	return a
}

func (m *Host) cleanup() {

	ticker := time.NewTicker(m.config.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.done:
			return
		case <-ticker.C:
			m.actors.RemoveIdle(m.config.idleTimeout)
			m.requests.RemoveExpired(m.config.requestTimeout)
		}
	}
}
