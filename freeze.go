package theatre

import (
	"context"
	"log/slog"
	"time"
)

// Freeze transitions the host into frozen state. This is called either
// explicitly or by the freeze monitor when lease health degrades.
//
// Sequence:
//  1. Set frozen flag → Send/Request return ErrHostFrozen, ActorForward responds HostFrozen.
//  2. Cancel freezeCtx → all actor contexts are cancelled.
//  3. Timeout all pending requests with ErrHostFrozen.
//  4. Wait grace period for actors to exit cleanly.
//  5. Force-stop remaining actors (close inbox, deregister, log warning).
//  6. Host remains frozen until Unfreeze or drain.
func (m *Host) Freeze() {
	if m.frozen.Load() {
		return // already frozen
	}

	slog.Warn("host entering frozen state", "host", m.hostRef.String())

	// Step 1: Set frozen flag. From this point, Send/Request return ErrHostFrozen.
	m.frozen.Store(true)
	m.metrics.FreezeCount.Add(1)

	// Step 1b: Cancel all scheduled messages — a frozen host should not fire them.
	m.scheduler.cancelAll()

	// Step 2: Cancel freezeCtx → all actor contexts are cancelled.
	m.freezeMu.Lock()
	m.freezeCancel()
	m.freezeMu.Unlock()

	// Step 3: Timeout all pending requests.
	m.requests.FailAll(ErrHostFrozen)

	// Step 4: Wait grace period for actors to exit cleanly.
	grace := m.config.freezeGracePeriod
	deadline := time.After(grace)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-deadline:
			goto forceStop
		case <-ticker.C:
			if m.actors.Count() == 0 {
				slog.Info("all actors exited during grace period")
				return
			}
		}
	}

forceStop:
	// Step 5: Force-stop remaining actors.
	remaining := m.actors.Count()
	if remaining > 0 {
		slog.Warn("force-stopping remaining actors after grace period",
			"count", remaining, "grace", grace)
		m.forceStopActors()
	}

	slog.Info("host frozen", "host", m.hostRef.String())
}

// Unfreeze attempts to restore the host to active state. Only succeeds if:
//   - Lease renewal succeeds (checked by caller or monitor).
//   - Epoch matches (no fencing occurred).
//   - Remaining lease >= safety margin.
//
// On success: creates a new freezeCtx, clears the frozen flag, empties
// the registry. No actors survive a freeze.
func (m *Host) Unfreeze() error {
	if !m.frozen.Load() {
		return nil // not frozen
	}

	if m.cluster != nil {
		// Verify lease health before unfreezing.
		remaining := m.cluster.RemainingLease()
		if remaining < m.config.safetyMargin {
			return ErrHostFrozen
		}

		// Attempt a lease renewal to confirm we're still valid.
		if err := m.cluster.renewLease(context.Background()); err != nil {
			slog.Error("unfreeze: lease renewal failed", "error", err)
			return ErrHostFrozen
		}

		// Re-check remaining lease after renewal.
		remaining = m.cluster.RemainingLease()
		if remaining < m.config.safetyMargin {
			slog.Warn("unfreeze: insufficient lease after renewal",
				"remaining", remaining, "safetyMargin", m.config.safetyMargin)
			return ErrHostFrozen
		}
	}

	slog.Info("host unfreezing", "host", m.hostRef.String())

	// Create new freeze context for future actors.
	m.freezeMu.Lock()
	m.freezeCtx, m.freezeCancel = context.WithCancel(context.Background())
	m.freezeMu.Unlock()

	// Ensure registry is empty (should be after freeze, but be safe).
	m.actors.ForceDeregisterAll()

	// Clear the frozen flag last — new actors can be created after this.
	m.frozen.Store(false)

	slog.Info("host unfrozen", "host", m.hostRef.String())
	return nil
}

// freezeMonitor runs in the background (cluster mode only) and checks
// lease health. If renewal failures or remaining lease cross thresholds,
// it triggers an automatic freeze.
func (m *Host) freezeMonitor() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-m.done:
			return
		case <-ticker.C:
			if m.frozen.Load() {
				// Already frozen. Check if lease expired → drain.
				if m.cluster.RemainingLease() == 0 {
					m.startDrain()
					return
				}
				continue
			}

			// Check freeze conditions.
			failures := m.cluster.ConsecutiveRenewalFailures()
			remaining := m.cluster.RemainingLease()

			if int(failures) >= m.config.maxRenewalFailures {
				slog.Warn("freeze monitor: renewal failures exceeded threshold",
					"failures", failures, "max", m.config.maxRenewalFailures)
				m.Freeze()
				continue
			}

			if remaining > 0 && remaining < m.config.safetyMargin {
				slog.Warn("freeze monitor: remaining lease below safety margin",
					"remaining", remaining, "margin", m.config.safetyMargin)
				m.Freeze()
			}
		}
	}
}

// startDrain is the terminal path: the lease has expired while frozen.
// Best-effort release of ownership rows, close transport listener, then stop.
func (m *Host) startDrain() {
	slog.Warn("host entering drain state (lease expired)", "host", m.hostRef.String())

	// Best-effort release of all ownership rows.
	if m.cluster != nil && m.cluster.DB() != nil {
		hostID := m.cluster.LocalHostID()
		epoch := m.cluster.LocalEpoch()
		_, err := m.cluster.DB().ExecContext(context.Background(), `
			DELETE FROM actor_ownership
			WHERE host_id = $1 AND epoch = $2
		`, hostID, epoch)
		if err != nil {
			slog.Error("drain: failed to release ownership rows", "error", err)
		} else {
			slog.Info("drain: released ownership rows", "hostID", hostID, "epoch", epoch)
		}
	}

	// Close transport listener to stop accepting new connections.
	if m.transport != nil {
		m.transport.Stop()
	}

	// Stop the host. This closes done, which terminates the freeze monitor.
	m.Stop()

	slog.Warn("host drained — must restart with new epoch", "host", m.hostRef.String())
}

// forceStopActors forcibly removes all actors from the registry and
// releases ownership. Used after the grace period when actors haven't
// exited cleanly in response to context cancellation.
func (m *Host) forceStopActors() {
	refs := m.actors.ForceDeregisterAll()
	for _, ref := range refs {
		slog.Warn("force-stopped actor", "type", ref.Type, "id", ref.ID)
		m.releaseOwnership(ref)
	}
}
