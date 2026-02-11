package theatre

import (
	"context"
	"database/sql"
)

// MigrateSchema creates the cluster tables if they do not exist.
// Safe to call on every startup — all statements use IF NOT EXISTS.
func MigrateSchema(ctx context.Context, db *sql.DB) error {
	const ddl = `
CREATE TABLE IF NOT EXISTS hosts (
	host_id      TEXT PRIMARY KEY,
	address      TEXT NOT NULL,
	epoch        BIGINT NOT NULL DEFAULT 1,
	lease_expiry TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS actor_ownership (
	actor_type TEXT NOT NULL,
	actor_id   TEXT NOT NULL,
	host_id    TEXT NOT NULL,
	epoch      BIGINT NOT NULL,
	claimed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	PRIMARY KEY (actor_type, actor_id)
);

CREATE TABLE IF NOT EXISTS schedules (
	schedule_id  BIGSERIAL PRIMARY KEY,
	actor_type   TEXT NOT NULL,
	actor_id     TEXT NOT NULL,
	body         BYTEA NOT NULL,
	cron_expr    TEXT,
	next_fire    TIMESTAMPTZ NOT NULL,
	one_shot     BOOLEAN NOT NULL DEFAULT false,
	created_by   TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_schedules_next_fire ON schedules (next_fire);
`
	_, err := db.ExecContext(ctx, ddl)
	return err
}
