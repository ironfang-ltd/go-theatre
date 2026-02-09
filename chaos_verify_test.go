package theatre

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// VerifyNoInterleaving checks the single-activation invariant:
// for each (actor_type, actor_id), the sequence of (host_id, host_epoch)
// that produced events must not interleave.
//
// Valid:   A(1), A(1), A(1), B(2), B(2) — contiguous runs.
// Invalid: A(1), B(2), A(1) — interleaving (A(1) reappears).
//
// Returns nil if the invariant holds, or an error describing the violation.
func VerifyNoInterleaving(ctx context.Context, db *sql.DB, runID string) error {
	rows, err := db.QueryContext(ctx, `
		SELECT actor_type, actor_id, host_id, host_epoch
		FROM audit_events
		WHERE run_id = $1
		ORDER BY actor_type, actor_id, ts, id
	`, runID)
	if err != nil {
		return fmt.Errorf("verify: query audit_events: %w", err)
	}
	defer rows.Close()

	type actorKey struct{ actorType, actorID string }
	type ownerKey struct {
		hostID string
		epoch  int64
	}

	// For each actor, track the current owner and all previously seen owners.
	current := make(map[actorKey]ownerKey)
	seen := make(map[actorKey]map[ownerKey]bool)

	for rows.Next() {
		var actorType, actorID, hostID string
		var epoch int64
		if err := rows.Scan(&actorType, &actorID, &hostID, &epoch); err != nil {
			return fmt.Errorf("verify: scan: %w", err)
		}

		ak := actorKey{actorType, actorID}
		ok := ownerKey{hostID, epoch}

		cur, exists := current[ak]
		if !exists {
			// First event for this actor.
			current[ak] = ok
			seen[ak] = map[ownerKey]bool{ok: true}
			continue
		}

		if cur == ok {
			// Same owner — contiguous run continues.
			continue
		}

		// Owner changed. Check if the new owner was seen before.
		if seen[ak][ok] {
			return fmt.Errorf(
				"INTERLEAVING DETECTED: actor %s/%s — owner (%s, epoch %d) "+
					"reappeared after transition to (%s, epoch %d)",
				actorType, actorID, ok.hostID, ok.epoch, cur.hostID, cur.epoch,
			)
		}

		// New owner — legitimate handoff.
		current[ak] = ok
		seen[ak][ok] = true
	}

	return rows.Err()
}

// VerifyNoPostFreezeProcessing checks that after a host enters Frozen state
// at freezeTime, no further "message" events appear from that host.
// Initialize events at freezeTime are allowed (they may have been in-flight).
func VerifyNoPostFreezeProcessing(ctx context.Context, db *sql.DB, runID, hostID string, freezeTime time.Time) error {
	var count int
	err := db.QueryRowContext(ctx, `
		SELECT count(*)
		FROM audit_events
		WHERE run_id = $1
		  AND host_id = $2
		  AND event = 'message'
		  AND ts > $3
	`, runID, hostID, freezeTime).Scan(&count)
	if err != nil {
		return fmt.Errorf("verify post-freeze: %w", err)
	}
	if count > 0 {
		return fmt.Errorf(
			"POST-FREEZE VIOLATION: host %s produced %d message events after freeze at %s",
			hostID, count, freezeTime.Format(time.RFC3339Nano),
		)
	}
	return nil
}
