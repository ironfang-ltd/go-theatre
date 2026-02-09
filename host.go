package theatre

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrUnregisteredActorType = fmt.Errorf("unregistered actor type")
	ErrHostDraining          = fmt.Errorf("host is draining")
	ErrHostFrozen            = fmt.Errorf("host is frozen")
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
	descriptors sync.Map // map[string]*Descriptor
	actors      *ActorRegistry
	requests    *RequestManager
	directory   Directory

	resPool sync.Pool
	outbox  chan OutboxMessage
	inbox   chan InboxMessage
	drain   chan struct{}
	done    chan struct{}

	// Cluster routing (nil in standalone mode).
	transport      *Transport
	cluster        *Cluster
	placementCache *PlacementCache

	// Pending remote requests awaiting reply or NotHere.
	pendingRemoteMu sync.Mutex
	pendingRemote   map[int64]*pendingRemoteRequest

	// Activation gate: deduplicates concurrent activations for the same Ref.
	activating sync.Map // map[Ref]*activationGate

	// Observability.
	metrics     *Metrics
	adminServer *AdminServer

	// Freeze state. Protected by freezeMu for ctx/cancel pair;
	// the frozen flag itself is atomic for lock-free fast-path checks.
	frozen      atomic.Bool
	freezeMu    sync.Mutex
	freezeCtx   context.Context
	freezeCancel context.CancelFunc

	// Stop idempotency.
	stopOnce sync.Once
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

	freezeCtx, freezeCancel := context.WithCancel(context.Background())

	metrics := newMetrics()

	h := &Host{
		hostRef:  hostRef,
		config:   cfg,
		requests: NewRequestManager(),
		directory:    NewDirectory(),
		actors:       NewActorManager(),
		resPool: sync.Pool{
			New: func() interface{} {
				return &Response{}
			},
		},
		outbox:        make(chan OutboxMessage, 512),
		inbox:         make(chan InboxMessage, cfg.hostInboxSize),
		drain:         make(chan struct{}),
		done:          make(chan struct{}),
		pendingRemote: make(map[int64]*pendingRemoteRequest),
		freezeCtx:    freezeCtx,
		freezeCancel: freezeCancel,
		metrics:      metrics,
	}

	metrics.actorCountFn = h.actors.Count

	return h
}

func (m *Host) Start() {

	slog.Info("starting", "host", m.hostRef.String())

	go m.cleanup()

	// In standalone mode, messages bypass the outbox entirely.
	if m.cluster != nil {
		go m.processOutbox()
	}

	for range m.config.inboxWorkers {
		go m.processInbox()
	}

	// Start freeze monitor in cluster mode.
	if m.cluster != nil {
		go m.freezeMonitor()
	}

	// Start admin server if configured.
	if m.config.adminAddr != "" {
		as, err := NewAdminServer(m, m.config.adminAddr)
		if err != nil {
			slog.Error("admin server failed to start", "error", err)
		} else {
			m.adminServer = as
			as.Start()
		}
	}
}

func (m *Host) Stop() {
	m.stopOnce.Do(func() {
		slog.Info("stopping", "host", m.hostRef.String())

		if m.adminServer != nil {
			m.adminServer.Stop()
		}

		// phase 1: close drain to reject new external messages
		close(m.drain)

		// wait for in-flight messages to be processed or timeout
		m.waitForDrain()

		// phase 2: close done to stop processing goroutines
		close(m.done)
		m.actors.RemoveAll()
	})
}

// IsFrozen returns whether the host is currently frozen.
func (m *Host) IsFrozen() bool {
	return m.frozen.Load()
}

// Metrics returns the host's operational metrics.
func (m *Host) Metrics() *Metrics {
	return m.metrics
}

func (m *Host) sendInternal(msg OutboxMessage) {
	if m.cluster == nil {
		// Standalone reply: resolve directly to the waiting request,
		// bypassing both outbox and inbox channels.
		if msg.IsReply {
			req := m.requests.Get(msg.ReplyID)
			if req != nil {
				res := m.resPool.Get().(*Response)
				res.Body = msg.Body
				res.Error = msg.Error
				req.Response <- res
			}
			return
		}

		// Standalone non-reply (actor-to-actor send): deliver directly
		// to existing actor or fall back to inbox for actor creation.
		if a := m.actors.Lookup(msg.RecipientRef); a != nil {
			a.Send(InboxMessage{
				SenderHostRef: m.hostRef,
				RecipientRef:  msg.RecipientRef,
				Body:          msg.Body,
				ReplyID:       msg.ReplyID,
			})
			return
		}
		m.inbox <- InboxMessage{
			SenderHostRef: m.hostRef,
			RecipientRef:  msg.RecipientRef,
			Body:          msg.Body,
			ReplyID:       msg.ReplyID,
		}
		return
	}
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
	m.descriptors.Store(name, &Descriptor{
		Name:   name,
		Create: creator,
	})
}

func (m *Host) hasDescriptor(typeName string) bool {
	_, ok := m.descriptors.Load(typeName)
	return ok
}

func (m *Host) getDescriptor(typeName string) *Descriptor {
	v, ok := m.descriptors.Load(typeName)
	if !ok {
		return nil
	}
	return v.(*Descriptor)
}

func (m *Host) Send(ref Ref, body interface{}) error {

	select {
	case <-m.drain:
		return ErrHostDraining
	default:
	}

	if m.frozen.Load() {
		return ErrHostFrozen
	}

	if m.cluster != nil {
		// Cluster mode: route through outbox.
		m.outbox <- OutboxMessage{
			RecipientRef: ref,
			Body:         body,
		}
		m.metrics.MessagesSent.Add(1)
		return nil
	}

	// Standalone mode.
	if !m.hasDescriptor(ref.Type) {
		return ErrUnregisteredActorType
	}

	msg := InboxMessage{
		SenderHostRef: m.hostRef,
		RecipientRef:  ref,
		Body:          body,
	}

	// Fast path: deliver directly to existing actor, skip inbox channel.
	if a := m.actors.Lookup(ref); a != nil {
		a.Send(msg)
		m.metrics.MessagesSent.Add(1)
		return nil
	}

	// Slow path: actor doesn't exist yet, route through inbox for creation.
	m.inbox <- msg
	m.metrics.MessagesSent.Add(1)
	return nil
}

func (m *Host) Request(ref Ref, body interface{}) (interface{}, error) {

	select {
	case <-m.drain:
		return nil, ErrHostDraining
	default:
	}

	if m.frozen.Load() {
		return nil, ErrHostFrozen
	}

	if m.cluster == nil && !m.hasDescriptor(ref.Type) {
		return nil, ErrUnregisteredActorType
	}

	m.metrics.RequestsTotal.Add(1)
	return m.requestInternal(ref, body)
}

func (m *Host) requestInternal(ref Ref, body interface{}) (interface{}, error) {

	// create a new request to track the response
	req := m.requests.Create(ref)

	msg := InboxMessage{
		SenderHostRef: m.hostRef,
		RecipientRef:  ref,
		Body:          body,
		ReplyID:       req.ID,
	}

	if m.cluster == nil {
		// Fast path: deliver directly to existing actor, skip inbox channel.
		if a := m.actors.Lookup(ref); a != nil {
			a.Send(msg)
		} else {
			// Slow path: actor doesn't exist yet, route through inbox.
			m.inbox <- msg
		}
	} else {
		m.outbox <- OutboxMessage{
			RecipientRef: ref,
			Body:         body,
			ReplyID:      req.ID,
		}
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
			m.metrics.MessagesReceived.Add(1)

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

			// When frozen, only deliver replies. Drop everything else.
			if m.frozen.Load() {
				m.metrics.MessagesDeadLettered.Add(1)
				if m.config.deadLetterHandler != nil {
					m.config.deadLetterHandler(msg)
				}
				continue
			}

			// try to find the actor in the local registry
			a := m.actors.Lookup(msg.RecipientRef)
			if a == nil {

				// In cluster mode, don't auto-create. The actor should
				// have been verified by routeMessage or spawned explicitly.
				if m.cluster != nil {
					slog.Warn("actor not found in cluster mode",
						"type", msg.RecipientRef.Type, "id", msg.RecipientRef.ID)
					if m.config.deadLetterHandler != nil {
						m.config.deadLetterHandler(msg)
					}
					continue
				}

				// Standalone mode: use activation gate for thread-safe
				// creation when multiple inbox workers are running.
				a = m.standaloneActivate(msg.RecipientRef)
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
	for {
		select {
		case <-m.done:
			return
		case msg := <-m.outbox:
			// When frozen, still drain replies but drop non-reply messages.
			if m.frozen.Load() && !msg.IsReply {
				if msg.ReplyID != 0 {
					m.failPendingRequest(msg.ReplyID, ErrHostFrozen)
				}
				continue
			}

			if msg.IsReply {
				m.routeReply(msg)
			} else if m.cluster != nil {
				m.routeMessage(msg)
			} else {
				m.deliverLocal(msg)
			}
		}
	}
}

func (m *Host) createLocalActor(ref Ref, reason ActivationReason) *Actor {

	d := m.getDescriptor(ref.Type)
	if d == nil {
		return nil
	}

	// Read the current freeze context under lock.
	m.freezeMu.Lock()
	parentCtx := m.freezeCtx
	m.freezeMu.Unlock()

	receiver := d.Create()

	a := NewActor(m, ref, receiver, parentCtx, m.config.actorInboxSize)
	a.onStop = func(r Ref) {
		m.actors.DeregisterOnly(r)
	}
	a.onDeactivate = func(r Ref) {
		m.releaseOwnership(r)
	}
	go a.Receive()

	a.Send(InboxMessage{
		SenderHostRef: m.hostRef,
		RecipientRef:  ref,
		Body:          Initialize{Reason: reason},
	})

	m.actors.Register(a)

	return a
}

// standaloneActivate creates an actor using the activation gate to
// deduplicate concurrent creation attempts from multiple inbox workers.
func (m *Host) standaloneActivate(ref Ref) *Actor {
	gate := &activationGate{done: make(chan struct{})}
	if existing, loaded := m.activating.LoadOrStore(ref, gate); loaded {
		// Another worker is already creating this actor. Wait for it.
		existingGate := existing.(*activationGate)
		<-existingGate.done
		return existingGate.actor
	}

	defer func() {
		close(gate.done)
		m.activating.Delete(ref)
	}()

	// Double-check: actor may have been registered while we waited.
	if a := m.actors.Lookup(ref); a != nil {
		gate.actor = a
		return a
	}

	a := m.createLocalActor(ref, ActivationNew)
	gate.actor = a
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
			expired := m.requests.RemoveExpired(m.config.requestTimeout)
			if expired > 0 {
				m.metrics.RequestsTimedOut.Add(int64(expired))
			}
		}
	}
}
