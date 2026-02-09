package theatre

import (
	"context"
	"database/sql"
	"fmt"
)

// --- audit schema ---

// CreateAuditSchema creates the audit_events table for chaos test verification.
// Safe to call multiple times (IF NOT EXISTS).
func CreateAuditSchema(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS audit_events (
	id          BIGSERIAL PRIMARY KEY,
	run_id      TEXT NOT NULL,
	actor_type  TEXT NOT NULL,
	actor_id    TEXT NOT NULL,
	host_id     TEXT NOT NULL,
	host_epoch  BIGINT NOT NULL,
	event       TEXT NOT NULL,
	ts          TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_audit_events_run ON audit_events (run_id, actor_type, actor_id, ts);
`)
	return err
}

// CleanAuditEvents deletes all audit events for a given run ID.
func CleanAuditEvents(ctx context.Context, db *sql.DB, runID string) {
	db.ExecContext(ctx, `DELETE FROM audit_events WHERE run_id = $1`, runID)
}

// AuditEvent records a single audit event into Postgres.
func AuditEvent(ctx context.Context, db *sql.DB, runID, actorType, actorID, hostID string, epoch int64, event string) error {
	_, err := db.ExecContext(ctx, `
		INSERT INTO audit_events (run_id, actor_type, actor_id, host_id, host_epoch, event)
		VALUES ($1, $2, $3, $4, $5, $6)
	`, runID, actorType, actorID, hostID, epoch, event)
	return err
}

// CountAuditEvents returns the number of audit events for a run.
func CountAuditEvents(ctx context.Context, db *sql.DB, runID string) (int, error) {
	var count int
	err := db.QueryRowContext(ctx, `SELECT count(*) FROM audit_events WHERE run_id = $1`, runID).Scan(&count)
	return count, err
}

// --- audit actor ---

// auditActor writes an audit event on Initialize and each user message.
// Used in chaos tests to track which host processed each message.
type auditActor struct {
	db      *sql.DB
	runID   string
	cluster *Cluster
}

func (a *auditActor) Receive(ctx *Context) error {
	hostID := a.cluster.LocalHostID()
	epoch := a.cluster.LocalEpoch()

	switch ctx.Message.(type) {
	case Initialize:
		if err := AuditEvent(ctx.Ctx, a.db, a.runID, ctx.ActorRef.Type, ctx.ActorRef.ID, hostID, epoch, "initialize"); err != nil {
			// Best-effort; don't fail the actor on audit write errors.
			fmt.Printf("audit write error (initialize): %v\n", err)
		}
		return nil
	case Shutdown:
		return nil
	}

	if err := AuditEvent(ctx.Ctx, a.db, a.runID, ctx.ActorRef.Type, ctx.ActorRef.ID, hostID, epoch, "message"); err != nil {
		fmt.Printf("audit write error (message): %v\n", err)
	}

	if ctx.replyId != 0 {
		ctx.Reply("ok")
	}
	return nil
}
