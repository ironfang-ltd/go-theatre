package theatre_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/gob"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ironfang-ltd/go-theatre"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// These tests require a live Postgres instance.
// Set THEATRE_TEST_DSN to a valid connection string, e.g.:
//
//	THEATRE_TEST_DSN="postgres://user:pass@localhost:5432/theatre_test?sslmode=disable"
//
// Run:
//
//	go test -v -run TestScheduler_Persist -timeout 60s ./...

func cleanSchedulesTable(t *testing.T, db *sql.DB) {
	t.Helper()
	db.ExecContext(context.Background(), `DELETE FROM schedules`)
}

func setupSchedulerHost(t *testing.T, db *sql.DB, hostID string, recv theatre.Receiver) *theatre.Host {
	t.Helper()

	cluster := theatre.NewCluster(db, theatre.ClusterConfig{
		HostID:        hostID,
		Address:       "127.0.0.1:0",
		LeaseDuration: 30 * time.Second,
		PollInterval:  1 * time.Second,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cluster.Start(ctx); err != nil {
		t.Fatalf("cluster.Start: %v", err)
	}

	h := theatre.NewHost(
		theatre.WithScheduleRecoveryInterval(500*time.Millisecond),
		theatre.WithIdleTimeout(30*time.Second),
	)
	h.RegisterActor("sched-test", func() theatre.Receiver { return recv })
	h.SetCluster(cluster)
	h.Start()

	t.Cleanup(func() {
		h.Stop()
		cluster.Stop()
	})

	return h
}

func TestScheduler_PersistOneShot(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	if err := theatre.MigrateSchema(ctx, db); err != nil {
		t.Fatalf("MigrateSchema: %v", err)
	}
	cleanTestTables(t, db)
	cleanSchedulesTable(t, db)

	got := make(chan struct{})
	var once sync.Once
	recv := &funcTestReceiver{fn: func(ctx *theatre.Context) {
		switch ctx.Message.(type) {
		case theatre.Initialize, theatre.Shutdown:
		default:
			once.Do(func() { close(got) })
		}
	}}

	h := setupSchedulerHost(t, db, "sched-oneshot-1", recv)

	ref := theatre.NewRef("sched-test", "s1")
	id, err := h.SendAfter(ref, "persist-test", 200*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if id == 0 {
		t.Fatal("expected non-zero schedule ID")
	}

	// Verify row exists in DB.
	var count int
	db.QueryRowContext(ctx, `SELECT count(*) FROM schedules WHERE schedule_id = $1`, int64(id)).Scan(&count)
	if count != 1 {
		t.Fatalf("expected 1 row in schedules, got %d", count)
	}

	// Wait for it to fire.
	select {
	case <-got:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for scheduled message")
	}

	// Verify row was deleted after firing.
	time.Sleep(200 * time.Millisecond)
	db.QueryRowContext(ctx, `SELECT count(*) FROM schedules WHERE schedule_id = $1`, int64(id)).Scan(&count)
	if count != 0 {
		t.Fatalf("expected row to be deleted after fire, got %d", count)
	}
}

func TestScheduler_PersistCron(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	if err := theatre.MigrateSchema(ctx, db); err != nil {
		t.Fatalf("MigrateSchema: %v", err)
	}
	cleanTestTables(t, db)
	cleanSchedulesTable(t, db)

	var fireCount atomic.Int64
	recv := &funcTestReceiver{fn: func(ctx *theatre.Context) {
		switch ctx.Message.(type) {
		case theatre.Initialize, theatre.Shutdown:
		default:
			fireCount.Add(1)
		}
	}}

	h := setupSchedulerHost(t, db, "sched-cron-1", recv)

	ref := theatre.NewRef("sched-test", "c1")
	id, err := h.SendCron(ref, "cron-test", "* * * * *")
	if err != nil {
		t.Fatal(err)
	}

	// Verify row exists with cron_expr.
	var cronExpr sql.NullString
	var nextFire time.Time
	err = db.QueryRowContext(ctx,
		`SELECT cron_expr, next_fire FROM schedules WHERE schedule_id = $1`, int64(id)).Scan(&cronExpr, &nextFire)
	if err != nil {
		t.Fatal(err)
	}
	if !cronExpr.Valid || cronExpr.String != "* * * * *" {
		t.Fatalf("expected cron_expr='* * * * *', got %v", cronExpr)
	}
	if nextFire.IsZero() {
		t.Fatal("expected non-zero next_fire")
	}

	// Cancel to clean up.
	if err := h.CancelSchedule(id); err != nil {
		t.Fatal(err)
	}

	// Verify row was deleted.
	var count int
	db.QueryRowContext(ctx, `SELECT count(*) FROM schedules WHERE schedule_id = $1`, int64(id)).Scan(&count)
	if count != 0 {
		t.Fatalf("expected row to be deleted after cancel, got %d", count)
	}
}

func TestScheduler_RecoveryAfterCrash(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	if err := theatre.MigrateSchema(ctx, db); err != nil {
		t.Fatalf("MigrateSchema: %v", err)
	}
	cleanTestTables(t, db)
	cleanSchedulesTable(t, db)

	// Host A creates a schedule then stops immediately (simulating crash).
	recvA := &funcTestReceiver{fn: func(ctx *theatre.Context) {}}
	hA := setupSchedulerHost(t, db, "sched-crash-A", recvA)

	ref := theatre.NewRef("sched-test", "r1")
	id, err := hA.SendAfter(ref, "recover-me", 100*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}

	// Verify it's in the DB.
	var count int
	db.QueryRowContext(ctx, `SELECT count(*) FROM schedules WHERE schedule_id = $1`, int64(id)).Scan(&count)
	if count != 1 {
		t.Fatalf("expected 1 row, got %d", count)
	}

	// Stop host A before the schedule fires.
	hA.Stop()

	// Wait for the schedule to become overdue.
	time.Sleep(300 * time.Millisecond)

	// Start host B — it should recover and fire the schedule.
	got := make(chan struct{})
	var once sync.Once
	recvB := &funcTestReceiver{fn: func(ctx *theatre.Context) {
		switch ctx.Message.(type) {
		case theatre.Initialize, theatre.Shutdown:
		default:
			once.Do(func() { close(got) })
		}
	}}

	clusterB := theatre.NewCluster(db, theatre.ClusterConfig{
		HostID:        "sched-crash-B",
		Address:       "127.0.0.1:0",
		LeaseDuration: 30 * time.Second,
		PollInterval:  1 * time.Second,
	})
	ctxB, cancelB := context.WithTimeout(ctx, 5*time.Second)
	defer cancelB()
	if err := clusterB.Start(ctxB); err != nil {
		t.Fatalf("clusterB.Start: %v", err)
	}

	hB := theatre.NewHost(
		theatre.WithScheduleRecoveryInterval(500*time.Millisecond),
		theatre.WithIdleTimeout(30*time.Second),
	)
	hB.RegisterActor("sched-test", func() theatre.Receiver { return recvB })
	hB.SetCluster(clusterB)
	hB.Start()
	defer func() {
		hB.Stop()
		clusterB.Stop()
	}()

	select {
	case <-got:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for recovered message on host B")
	}

	// Verify schedule was deleted from DB (one-shot).
	db.QueryRowContext(ctx, `SELECT count(*) FROM schedules WHERE schedule_id = $1`, int64(id)).Scan(&count)
	if count != 0 {
		t.Fatalf("expected row to be deleted after recovery, got %d", count)
	}

	if hB.Metrics().SchedulesRecovered.Load() < 1 {
		t.Fatal("expected SchedulesRecovered >= 1")
	}
}

func TestScheduler_NoDuplicateFire(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	if err := theatre.MigrateSchema(ctx, db); err != nil {
		t.Fatalf("MigrateSchema: %v", err)
	}
	cleanTestTables(t, db)
	cleanSchedulesTable(t, db)

	// Insert a schedule directly into DB (overdue).
	var schedID int64
	err := db.QueryRowContext(ctx, `
		INSERT INTO schedules (actor_type, actor_id, body, next_fire, one_shot, created_by)
		VALUES ($1, $2, $3, now() - interval '1 second', true, 'test')
		RETURNING schedule_id
	`, "sched-test", "d1", mustEncodeBody(t, "dedup-test")).Scan(&schedID)
	if err != nil {
		t.Fatal(err)
	}

	var totalFires atomic.Int64

	// Start two hosts that both poll for overdue schedules.
	makeRecv := func() *funcTestReceiver {
		return &funcTestReceiver{fn: func(ctx *theatre.Context) {
			switch ctx.Message.(type) {
			case theatre.Initialize, theatre.Shutdown:
			default:
				totalFires.Add(1)
			}
		}}
	}

	_ = setupSchedulerHost(t, db, "sched-dedup-1", makeRecv())
	_ = setupSchedulerHost(t, db, "sched-dedup-2", makeRecv())

	// Wait for recovery polls to run.
	time.Sleep(2 * time.Second)

	fires := totalFires.Load()
	if fires != 1 {
		t.Fatalf("expected exactly 1 fire, got %d", fires)
	}
}

func TestScheduler_CancelPersistent(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	if err := theatre.MigrateSchema(ctx, db); err != nil {
		t.Fatalf("MigrateSchema: %v", err)
	}
	cleanTestTables(t, db)
	cleanSchedulesTable(t, db)

	recv := &funcTestReceiver{fn: func(ctx *theatre.Context) {}}
	h := setupSchedulerHost(t, db, "sched-cancel-1", recv)

	ref := theatre.NewRef("sched-test", "x1")
	id, err := h.SendAfter(ref, "cancel-me", 10*time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	// Verify row exists.
	var count int
	db.QueryRowContext(ctx, `SELECT count(*) FROM schedules WHERE schedule_id = $1`, int64(id)).Scan(&count)
	if count != 1 {
		t.Fatalf("expected 1 row, got %d", count)
	}

	// Cancel.
	if err := h.CancelSchedule(id); err != nil {
		t.Fatal(err)
	}

	// Verify row was deleted.
	db.QueryRowContext(ctx, `SELECT count(*) FROM schedules WHERE schedule_id = $1`, int64(id)).Scan(&count)
	if count != 0 {
		t.Fatalf("expected 0 rows after cancel, got %d", count)
	}
}

func TestScheduler_FreezeKeepsDBRows(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	if err := theatre.MigrateSchema(ctx, db); err != nil {
		t.Fatalf("MigrateSchema: %v", err)
	}
	cleanTestTables(t, db)
	cleanSchedulesTable(t, db)

	recv := &funcTestReceiver{fn: func(ctx *theatre.Context) {}}
	h := setupSchedulerHost(t, db, "sched-freeze-1", recv)

	ref := theatre.NewRef("sched-test", "f1")
	id, err := h.SendAfter(ref, "freeze-test", 10*time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	// Verify row exists.
	var count int
	db.QueryRowContext(ctx, `SELECT count(*) FROM schedules WHERE schedule_id = $1`, int64(id)).Scan(&count)
	if count != 1 {
		t.Fatalf("expected 1 row, got %d", count)
	}

	// Freeze the host — should clear local map but NOT DB rows.
	h.Freeze()

	// Verify DB row still exists.
	db.QueryRowContext(ctx, `SELECT count(*) FROM schedules WHERE schedule_id = $1`, int64(id)).Scan(&count)
	if count != 1 {
		t.Fatalf("expected DB row to survive freeze, got %d", count)
	}
}

// --- test helpers ---

type funcTestReceiver struct {
	fn func(ctx *theatre.Context)
}

func (r *funcTestReceiver) Receive(ctx *theatre.Context) error {
	r.fn(ctx)
	return nil
}

func mustEncodeBody(t *testing.T, body interface{}) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&body); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}
