package theatre

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
)

// ErrClaimLost is returned when another host holds valid ownership for an actor.
var ErrClaimLost = fmt.Errorf("ownership claim lost to another host")

// ClaimResult describes the outcome of a claimOwnership call.
type ClaimResult struct {
	OwnerHostID string
	OwnerEpoch  int64
	Won         bool
	Reason      ActivationReason
}

// activationGate deduplicates concurrent activation attempts for the same actor.
// Stored in Host.activating (sync.Map keyed by Ref).
type activationGate struct {
	done  chan struct{}
	actor *Actor
	err   error
}

// activateActor attempts to activate an actor on this host.
// If claim is true, ownership is claimed from the database first.
// If claim is false, the caller already verified we own the actor.
// Uses an activation gate (sync.Map) to deduplicate concurrent attempts.
func (m *Host) activateActor(ref Ref, claim bool) (*Actor, error) {
	// Fail fast if host is frozen — no new activations allowed.
	if m.frozen.Load() {
		return nil, ErrHostFrozen
	}

	gate := &activationGate{done: make(chan struct{})}
	if existing, loaded := m.activating.LoadOrStore(ref, gate); loaded {
		existingGate := existing.(*activationGate)
		<-existingGate.done
		return existingGate.actor, existingGate.err
	}

	defer func() {
		close(gate.done)
		m.activating.Delete(ref)
	}()

	// Double-check: actor may have been created while we waited.
	if a := m.actors.Lookup(ref); a != nil {
		gate.actor = a
		return a, nil
	}

	if !m.hasDescriptor(ref.Type) {
		gate.err = ErrUnregisteredActorType
		return nil, gate.err
	}

	var reason ActivationReason

	if claim {
		result, err := m.claimOwnership(ref)
		if err != nil {
			gate.err = err
			return nil, err
		}
		if result == nil || !result.Won {
			gate.err = ErrClaimLost
			return nil, gate.err
		}
		reason = result.Reason
	} else {
		reason = ActivationReactivation
	}

	// Test hook: sleep between claim and actor start (chaos test injection point).
	if m.config.postClaimHook != nil {
		m.config.postClaimHook(ref)
	}

	a := m.createLocalActor(ref, reason)
	if a == nil {
		m.metrics.ActivationsFailed.Add(1)
		gate.err = fmt.Errorf("failed to create actor %s", ref)
		return nil, gate.err
	}

	gate.actor = a
	return a, nil
}

// claimOwnership attempts to atomically claim ownership of an actor.
// Uses INSERT ... ON CONFLICT DO UPDATE with a WHERE guard that only
// allows the update when no live host currently owns the actor.
// Returns the claim result indicating whether this host won.
func (m *Host) claimOwnership(ref Ref) (*ClaimResult, error) {
	if m.cluster == nil {
		return nil, nil
	}
	if m.cluster.DB() == nil {
		return &ClaimResult{
			OwnerHostID: m.cluster.LocalHostID(),
			OwnerEpoch:  m.cluster.LocalEpoch(),
			Won:         true,
			Reason:      ActivationNew,
		}, nil
	}

	hostID := m.cluster.LocalHostID()
	epoch := m.cluster.LocalEpoch()

	var ownerHostID string
	var ownerEpoch int64
	var prevHostID sql.NullString
	var prevEpoch sql.NullInt64

	err := m.cluster.DB().QueryRowContext(context.Background(), `
		WITH old AS (
			SELECT host_id, epoch FROM actor_ownership
			WHERE actor_type = $1 AND actor_id = $2
		)
		INSERT INTO actor_ownership (actor_type, actor_id, host_id, epoch)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (actor_type, actor_id) DO UPDATE
			SET host_id    = EXCLUDED.host_id,
			    epoch      = EXCLUDED.epoch,
			    claimed_at = now()
			WHERE NOT EXISTS (
				SELECT 1 FROM hosts h
				WHERE h.host_id = actor_ownership.host_id
				  AND h.epoch   = actor_ownership.epoch
				  AND h.lease_expiry > now()
			)
		RETURNING host_id, epoch,
			(SELECT host_id FROM old) AS prev_host_id,
			(SELECT epoch FROM old) AS prev_epoch
	`, ref.Type, ref.ID, hostID, epoch).Scan(&ownerHostID, &ownerEpoch, &prevHostID, &prevEpoch)

	if err == sql.ErrNoRows {
		// ON CONFLICT matched but WHERE clause was false → existing valid owner.
		// Resolve who actually owns it.
		owner, resolveErr := m.resolveOwner(ref)
		if resolveErr != nil {
			return nil, resolveErr
		}
		if owner != nil {
			return &ClaimResult{
				OwnerHostID: owner.HostID,
				OwnerEpoch:  owner.Epoch,
				Won:         false,
			}, nil
		}
		// Rare: owner disappeared between our claim and resolve.
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	won := ownerHostID == hostID && ownerEpoch == epoch

	var reason ActivationReason
	if !prevHostID.Valid {
		reason = ActivationNew
	} else if prevHostID.String == hostID {
		reason = ActivationReactivation
	} else {
		reason = ActivationFailover
	}

	return &ClaimResult{
		OwnerHostID: ownerHostID,
		OwnerEpoch:  ownerEpoch,
		Won:         won,
		Reason:      reason,
	}, nil
}

// releaseOwnership deletes the actor_ownership row, guarded by host_id
// and epoch to prevent deleting another host's claim.
func (m *Host) releaseOwnership(ref Ref) {
	if m.cluster == nil || m.cluster.DB() == nil {
		return
	}

	hostID := m.cluster.LocalHostID()
	epoch := m.cluster.LocalEpoch()

	_, err := m.cluster.DB().ExecContext(context.Background(), `
		DELETE FROM actor_ownership
		WHERE actor_type = $1 AND actor_id = $2
		  AND host_id = $3 AND epoch = $4
	`, ref.Type, ref.ID, hostID, epoch)
	if err != nil {
		slog.Error("release ownership failed", "ref", ref, "error", err)
	}
}
