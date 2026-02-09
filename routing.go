package theatre

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
)

// ErrNoOwner is returned when no live owner can be found for an actor.
var ErrNoOwner = fmt.Errorf("no owner found for actor")

// pendingRemoteRequest tracks a forwarded request so it can be retried
// if the target responds with NotHere.
type pendingRemoteRequest struct {
	msg     OutboxMessage
	ref     Ref
	retried bool
}

// SetTransport wires the transport into this host. Must be called before Start.
func (m *Host) SetTransport(t *Transport) {
	m.transport = t
}

// SetCluster wires the cluster into this host and creates a placement cache.
// Must be called before Start.
func (m *Host) SetCluster(c *Cluster) {
	m.cluster = c
	m.placementCache = newPlacementCache(m.config.placementTTL)
}

// HandleTransportMessage is the TransportHandler for this host.
// Pass it to NewTransport as the handler callback.
func (m *Host) HandleTransportMessage(fromHostID string, env TransportEnvelope) {
	switch msg := env.Payload.(type) {
	case *ActorForward:
		m.handleActorForward(fromHostID, msg)
	case *ActorForwardReply:
		m.handleActorForwardReply(msg)
	case *NotHere:
		m.handleNotHere(msg)
	case *HostFrozen:
		m.handleHostFrozen(msg)
	default:
		slog.Warn("host received unhandled transport message", "tag", env.Tag, "from", fromHostID)
	}
}

// SpawnLocal explicitly creates a local actor. Used when an actor needs
// to be started on this host without going through the normal routing/
// activation path (e.g. in tests or manual placement).
func (m *Host) SpawnLocal(ref Ref) error {
	if !m.hasDescriptor(ref.Type) {
		return ErrUnregisteredActorType
	}
	if a := m.actors.Lookup(ref); a != nil {
		return nil // already exists
	}
	a := m.createLocalActor(ref, ActivationNew)
	if a == nil {
		return fmt.Errorf("failed to create actor %s", ref)
	}
	return nil
}

// --- outbox routing ---

// routeMessage routes a non-reply outbox message.
// In cluster mode it checks local registry → placement cache → DB.
// In standalone mode it delivers to the inbox for local processing.
func (m *Host) routeMessage(msg OutboxMessage) {
	ref := msg.RecipientRef

	// 1. Local actor exists → deliver directly.
	if a := m.actors.Lookup(ref); a != nil {
		a.Send(InboxMessage{
			SenderHostRef: m.hostRef,
			RecipientRef:  ref,
			Body:          msg.Body,
			ReplyID:       msg.ReplyID,
		})
		return
	}

	// 2. Placement cache hit.
	if m.placementCache != nil {
		if entry, ok := m.placementCache.Get(ref); ok {
			m.metrics.PlacementCacheHits.Add(1)
			if m.isEntryLive(entry) {
				if entry.HostID == m.cluster.LocalHostID() {
					// Cache says local but actor gone → evict, fall through.
					m.placementCache.Evict(ref)
				} else {
					if err := m.forwardToRemote(entry.HostID, entry.Address, ref, msg); err != nil {
						slog.Warn("forward failed, evicting cache",
							"ref", ref, "target", entry.HostID, "error", err)
						m.placementCache.Evict(ref)
						m.resolveAndForward(ref, msg)
					}
					return
				}
			} else {
				// Epoch mismatch or host not live → evict.
				m.placementCache.Evict(ref)
			}
		}
	}

	// Cache miss — either no entry, expired, or evicted above.
	if m.placementCache != nil {
		m.metrics.PlacementCacheMisses.Add(1)
	}

	// 3. Resolve from DB (if cluster is available).
	if m.cluster != nil {
		m.resolveAndForward(ref, msg)
		return
	}

	// 4. Standalone mode — deliver to inbox (processInbox will auto-create).
	m.deliverLocal(msg)
}

// routeReply routes a reply message back to the originating host.
func (m *Host) routeReply(msg OutboxMessage) {
	// Local reply (standalone or same host).
	if msg.recipientHostID == "" || m.transport == nil {
		m.deliverLocal(msg)
		return
	}

	// Remote reply via transport.
	replyMsg := ActorForwardReply{
		ReplyID: msg.ReplyID,
		Body:    msg.Body,
	}
	if msg.Error != nil {
		replyMsg.Error = msg.Error.Error()
	}
	env, err := Envelope(replyMsg)
	if err != nil {
		slog.Error("envelope error for reply", "error", err)
		return
	}
	if err := m.transport.SendTo(msg.recipientHostID, msg.recipientAddress, env); err != nil {
		slog.Error("transport reply failed",
			"recipientHostID", msg.recipientHostID, "error", err)
	}
}

// deliverLocal puts a message on the host inbox for local processing.
func (m *Host) deliverLocal(msg OutboxMessage) {
	m.inbox <- InboxMessage{
		SenderHostRef: m.hostRef,
		RecipientRef:  msg.RecipientRef,
		Body:          msg.Body,
		ReplyID:       msg.ReplyID,
		IsReply:       msg.IsReply,
		Error:         msg.Error,
	}
}

// --- remote forwarding ---

func (m *Host) forwardToRemote(hostID, address string, ref Ref, msg OutboxMessage) error {
	fwd := ActorForward{
		ActorType:    ref.Type,
		ActorID:      ref.ID,
		Body:         msg.Body,
		ReplyID:      msg.ReplyID,
		SenderHostID: m.cluster.LocalHostID(),
	}
	env, err := Envelope(fwd)
	if err != nil {
		return err
	}
	if msg.ReplyID != 0 {
		m.storePendingRemote(msg.ReplyID, ref, msg)
	}
	if err := m.transport.SendTo(hostID, address, env); err != nil {
		if msg.ReplyID != 0 {
			m.removePendingRemote(msg.ReplyID)
		}
		return err
	}
	return nil
}

func (m *Host) resolveAndForward(ref Ref, msg OutboxMessage) {
	owner, err := m.resolveOwner(ref)
	if err != nil {
		slog.Error("resolve owner failed", "ref", ref, "error", err)
		m.handleDeadLetter(msg)
		return
	}

	if owner != nil {
		m.placementCache.Put(ref, *owner)

		if owner.HostID == m.cluster.LocalHostID() {
			// Owner is us but actor doesn't exist locally.
			// Re-activate without re-claiming (we already own it).
			m.activateAndDeliver(ref, false, msg)
			return
		}

		if err := m.forwardToRemote(owner.HostID, owner.Address, ref, msg); err != nil {
			slog.Error("forward to resolved owner failed", "ref", ref, "error", err)
			m.handleDeadLetter(msg)
		}
		return
	}

	// No valid owner. Need a DB to establish ownership via claim.
	if m.cluster.DB() == nil {
		m.handleDeadLetter(msg)
		return
	}

	// Use ring to determine preferred host.
	ring := m.cluster.Ring()
	if ring == nil {
		m.handleDeadLetter(msg)
		return
	}
	ringKey := ref.Type + ":" + ref.ID
	preferredHost, ok := ring.Lookup(ringKey)
	if !ok {
		m.handleDeadLetter(msg)
		return
	}

	if preferredHost == m.cluster.LocalHostID() {
		// We're the preferred host. Claim and activate.
		m.activateAndDeliver(ref, true, msg)
	} else {
		// Forward to the preferred host (they will claim).
		address := m.getHostAddress(preferredHost)
		if address == "" {
			slog.Warn("preferred host address not found", "host", preferredHost)
			m.handleDeadLetter(msg)
			return
		}
		if err := m.forwardToRemote(preferredHost, address, ref, msg); err != nil {
			slog.Error("forward to preferred host failed", "ref", ref, "error", err)
			m.handleDeadLetter(msg)
		}
	}
}

// activateAndDeliver runs the activation gate and delivers the outbox
// message to the newly (or already) activated actor.
func (m *Host) activateAndDeliver(ref Ref, claim bool, msg OutboxMessage) {
	a, err := m.activateActor(ref, claim)
	if err != nil {
		slog.Warn("activation failed", "ref", ref, "error", err)
		m.handleDeadLetter(msg)
		return
	}
	a.Send(InboxMessage{
		SenderHostRef: m.hostRef,
		RecipientRef:  ref,
		Body:          msg.Body,
		ReplyID:       msg.ReplyID,
	})
}

// --- DB ownership resolution (read-only) ---

func (m *Host) resolveOwner(ref Ref) (*PlacementEntry, error) {
	if m.cluster == nil || m.cluster.DB() == nil {
		return nil, nil
	}

	var hostID, address string
	var ownerEpoch, hostEpoch int64

	err := m.cluster.DB().QueryRowContext(context.Background(), `
		SELECT ao.host_id, ao.epoch, h.address, h.epoch AS host_epoch
		FROM actor_ownership ao
		JOIN hosts h ON ao.host_id = h.host_id
		WHERE ao.actor_type = $1 AND ao.actor_id = $2
		  AND h.lease_expiry > now()
		  AND h.epoch = ao.epoch
	`, ref.Type, ref.ID).Scan(&hostID, &ownerEpoch, &address, &hostEpoch)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	return &PlacementEntry{
		HostID:  hostID,
		Address: address,
		Epoch:   ownerEpoch,
	}, nil
}

// isEntryLive validates a placement entry against the cluster's live host list.
func (m *Host) isEntryLive(entry PlacementEntry) bool {
	if m.cluster == nil {
		return true // no liveness info → trust the cache
	}
	for _, h := range m.cluster.LiveHosts() {
		if h.HostID == entry.HostID {
			return h.Epoch == entry.Epoch
		}
	}
	return false
}

// getHostAddress looks up a host's address from the liveness cache.
func (m *Host) getHostAddress(hostID string) string {
	if m.cluster == nil {
		return ""
	}
	for _, h := range m.cluster.LiveHosts() {
		if h.HostID == hostID {
			return h.Address
		}
	}
	return ""
}

// --- inbound transport handlers ---

func (m *Host) handleActorForward(fromHostID string, msg *ActorForward) {
	// If frozen, reject with HostFrozen so the sender can re-route.
	if m.frozen.Load() {
		m.sendHostFrozen(fromHostID, msg)
		return
	}

	ref := NewRef(msg.ActorType, msg.ActorID)
	a := m.actors.Lookup(ref)

	if a == nil && m.cluster != nil && m.hasDescriptor(ref.Type) {
		// Actor not running locally. Try to claim and activate.
		var err error
		a, err = m.activateActor(ref, true)
		if err != nil {
			slog.Info("activation on forward failed, sending NotHere",
				"ref", ref, "from", fromHostID, "error", err)
		}
	}

	if a == nil {
		m.sendNotHere(fromHostID, msg)
		return
	}

	senderAddress := m.getHostAddress(fromHostID)

	a.Send(InboxMessage{
		RecipientRef:  ref,
		Body:          msg.Body,
		ReplyID:       msg.ReplyID,
		senderHostID:  fromHostID,
		senderAddress: senderAddress,
	})
}

func (m *Host) handleActorForwardReply(msg *ActorForwardReply) {
	m.removePendingRemote(msg.ReplyID)

	req := m.requests.Get(msg.ReplyID)
	if req == nil {
		slog.Warn("received reply for unknown request", "replyID", msg.ReplyID)
		return
	}

	res := m.resPool.Get().(*Response)
	res.Body = msg.Body
	if msg.Error != "" {
		res.Error = fmt.Errorf("%s", msg.Error)
	} else {
		res.Error = nil
	}
	req.Response <- res
}

func (m *Host) handleNotHere(msg *NotHere) {
	ref := NewRef(msg.ActorType, msg.ActorID)
	slog.Info("received NotHere", "actor", ref, "fromHost", msg.HostID)

	if m.placementCache != nil {
		m.placementCache.Evict(ref)
	}

	m.retryPendingForActor(ref)
}

func (m *Host) sendNotHere(toHostID string, fwd *ActorForward) {
	if m.transport == nil || m.cluster == nil {
		return
	}
	nh := NotHere{
		ActorType: fwd.ActorType,
		ActorID:   fwd.ActorID,
		HostID:    m.cluster.LocalHostID(),
		Epoch:     m.cluster.LocalEpoch(),
	}
	env, err := Envelope(nh)
	if err != nil {
		slog.Error("envelope error for NotHere", "error", err)
		return
	}
	address := m.getHostAddress(toHostID)
	if err := m.transport.SendTo(toHostID, address, env); err != nil {
		slog.Error("failed to send NotHere", "to", toHostID, "error", err)
	}
}

func (m *Host) sendHostFrozen(toHostID string, fwd *ActorForward) {
	if m.transport == nil || m.cluster == nil {
		return
	}
	hf := HostFrozen{
		ActorType: fwd.ActorType,
		ActorID:   fwd.ActorID,
		ReplyID:   fwd.ReplyID,
		HostID:    m.cluster.LocalHostID(),
		Epoch:     m.cluster.LocalEpoch(),
	}
	env, err := Envelope(hf)
	if err != nil {
		slog.Error("envelope error for HostFrozen", "error", err)
		return
	}
	address := m.getHostAddress(toHostID)
	if err := m.transport.SendTo(toHostID, address, env); err != nil {
		slog.Error("failed to send HostFrozen", "to", toHostID, "error", err)
	}
}

// handleHostFrozen is called when a remote host tells us it is frozen.
// Treat like NotHere — evict cache and retry pending requests.
func (m *Host) handleHostFrozen(msg *HostFrozen) {
	ref := NewRef(msg.ActorType, msg.ActorID)
	slog.Info("received HostFrozen", "actor", ref, "fromHost", msg.HostID)

	if m.placementCache != nil {
		m.placementCache.Evict(ref)
	}

	m.retryPendingForActor(ref)
}

// --- pending remote request tracking ---

func (m *Host) storePendingRemote(replyID int64, ref Ref, msg OutboxMessage) {
	m.pendingRemoteMu.Lock()
	m.pendingRemote[replyID] = &pendingRemoteRequest{msg: msg, ref: ref}
	m.pendingRemoteMu.Unlock()
}

func (m *Host) removePendingRemote(replyID int64) {
	m.pendingRemoteMu.Lock()
	delete(m.pendingRemote, replyID)
	m.pendingRemoteMu.Unlock()
}

// retryPendingForActor re-resolves ownership and retries any pending
// remote requests for the given actor. Max 1 retry per request.
func (m *Host) retryPendingForActor(ref Ref) {
	m.pendingRemoteMu.Lock()
	var toRetry []*pendingRemoteRequest
	var toFail []*pendingRemoteRequest
	for _, pr := range m.pendingRemote {
		if pr.ref == ref {
			if !pr.retried {
				pr.retried = true
				toRetry = append(toRetry, pr)
			} else {
				toFail = append(toFail, pr)
			}
		}
	}
	m.pendingRemoteMu.Unlock()

	// Fail requests that already had their one retry.
	for _, pr := range toFail {
		m.failPendingRequest(pr.msg.ReplyID, ErrNoOwner)
	}

	if len(toRetry) == 0 {
		return
	}

	// Re-resolve from DB.
	owner, err := m.resolveOwner(ref)
	if err != nil {
		slog.Error("re-resolve failed", "ref", ref, "error", err)
	}

	for _, pr := range toRetry {
		if owner != nil {
			if m.placementCache != nil {
				m.placementCache.Put(ref, *owner)
			}
			if owner.HostID == m.cluster.LocalHostID() {
				m.deliverLocal(pr.msg)
			} else {
				if err := m.forwardToRemote(owner.HostID, owner.Address, ref, pr.msg); err != nil {
					m.failPendingRequest(pr.msg.ReplyID, ErrNoOwner)
				}
			}
		} else {
			m.failPendingRequest(pr.msg.ReplyID, ErrNoOwner)
		}
	}
}

func (m *Host) failPendingRequest(replyID int64, err error) {
	m.pendingRemoteMu.Lock()
	delete(m.pendingRemote, replyID)
	m.pendingRemoteMu.Unlock()

	req := m.requests.Get(replyID)
	if req != nil {
		res := m.resPool.Get().(*Response)
		res.Error = err
		req.Response <- res
	}
}

// --- dead letter ---

func (m *Host) handleDeadLetter(msg OutboxMessage) {
	m.metrics.MessagesDeadLettered.Add(1)
	slog.Warn("dead letter", "type", msg.RecipientRef.Type, "id", msg.RecipientRef.ID)

	if msg.ReplyID != 0 {
		m.failPendingRequest(msg.ReplyID, ErrNoOwner)
	}
	if m.config.deadLetterHandler != nil {
		m.config.deadLetterHandler(InboxMessage{
			RecipientRef: msg.RecipientRef,
			Body:         msg.Body,
			ReplyID:      msg.ReplyID,
		})
	}
}
