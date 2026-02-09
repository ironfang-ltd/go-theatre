package theatre_test

import (
	"context"
	"database/sql"
	"os"
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
//	go test -v -run TestCluster -timeout 60s ./...

func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("THEATRE_TEST_DSN")
	if dsn == "" {
		t.Skip("THEATRE_TEST_DSN not set — skipping cluster integration test")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("db.Ping: %v", err)
	}
	return db
}

func cleanTestTables(t *testing.T, db *sql.DB) {
	t.Helper()
	ctx := context.Background()
	db.ExecContext(ctx, `DELETE FROM actor_ownership`)
	db.ExecContext(ctx, `DELETE FROM hosts`)
}

func TestCluster_ThreeHosts(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	if err := theatre.MigrateSchema(ctx, db); err != nil {
		t.Fatalf("MigrateSchema: %v", err)
	}
	cleanTestTables(t, db)

	// Use short durations so the test runs quickly.
	lease := 4 * time.Second
	poll := 500 * time.Millisecond

	// --- Start 3 hosts ---
	clusters := make([]*theatre.Cluster, 3)
	for i := range clusters {
		cfg := theatre.ClusterConfig{
			HostID:        hostID(i),
			Address:       addr(i),
			LeaseDuration: lease,
			PollInterval:  poll,
		}
		c := theatre.NewCluster(db, cfg)
		if err := c.Start(ctx); err != nil {
			t.Fatalf("cluster %d Start: %v", i, err)
		}
		clusters[i] = c
	}
	defer func() {
		for _, c := range clusters {
			if c != nil {
				c.Stop()
			}
		}
	}()

	// Wait for polls to converge.
	time.Sleep(2 * poll)

	// All three hosts should appear in the ring.
	members := clusters[0].Ring().Members()
	if len(members) != 3 {
		t.Fatalf("expected 3 ring members, got %d: %v", len(members), members)
	}
	t.Logf("ring members after 3-host start: %v", members)

	// Verify epochs are all ≥ 1.
	for i, c := range clusters {
		e := c.LocalEpoch()
		if e < 1 {
			t.Fatalf("cluster %d epoch=%d, want ≥1", i, e)
		}
		t.Logf("cluster %d: epoch=%d remaining=%s", i, e, c.RemainingLease().Round(time.Millisecond))
	}

	// Verify all hosts are live.
	live := clusters[0].LiveHosts()
	if len(live) != 3 {
		t.Fatalf("expected 3 live hosts, got %d", len(live))
	}

	// --- Lookup is deterministic ---
	host1, ok1 := clusters[0].Ring().Lookup("player:42")
	host2, ok2 := clusters[1].Ring().Lookup("player:42")
	if !ok1 || !ok2 {
		t.Fatal("ring lookup failed")
	}
	if host1 != host2 {
		t.Fatalf("ring not deterministic: %s vs %s", host1, host2)
	}
	t.Logf("player:42 → %s (deterministic across hosts)", host1)

	// --- Stop host-2, wait for lease to expire, verify ring shrinks ---
	epoch2Before := clusters[2].LocalEpoch()
	clusters[2].Stop()
	clusters[2] = nil

	// Wait for host-2's lease to expire + a poll cycle.
	time.Sleep(lease + 2*poll)

	members = clusters[0].Ring().Members()
	if len(members) != 2 {
		t.Fatalf("expected 2 ring members after host-2 stops, got %d: %v", len(members), members)
	}
	t.Logf("ring members after host-2 stops: %v", members)

	// --- Restart host-2 → epoch must bump ---
	cfg2 := theatre.ClusterConfig{
		HostID:        hostID(2),
		Address:       addr(2),
		LeaseDuration: lease,
		PollInterval:  poll,
	}
	c2 := theatre.NewCluster(db, cfg2)
	if err := c2.Start(ctx); err != nil {
		t.Fatalf("cluster 2 restart Start: %v", err)
	}
	clusters[2] = c2

	epoch2After := c2.LocalEpoch()
	if epoch2After <= epoch2Before {
		t.Fatalf("epoch did not bump on restart: before=%d after=%d", epoch2Before, epoch2After)
	}
	t.Logf("host-2 epoch bumped: %d → %d", epoch2Before, epoch2After)

	// Wait for polls to converge.
	time.Sleep(2 * poll)

	members = clusters[0].Ring().Members()
	if len(members) != 3 {
		t.Fatalf("expected 3 ring members after restart, got %d: %v", len(members), members)
	}
	t.Logf("ring members after host-2 restart: %v", members)
}

func TestCluster_AdvisoryLockConflict(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	if err := theatre.MigrateSchema(ctx, db); err != nil {
		t.Fatalf("MigrateSchema: %v", err)
	}
	cleanTestTables(t, db)

	cfg := theatre.ClusterConfig{
		HostID:        "conflict-host",
		Address:       "127.0.0.1:9000",
		LeaseDuration: 10 * time.Second,
		PollInterval:  1 * time.Second,
	}

	c1 := theatre.NewCluster(db, cfg)
	if err := c1.Start(ctx); err != nil {
		t.Fatalf("c1 Start: %v", err)
	}
	defer c1.Stop()

	// Second process with same host ID should fail.
	c2 := theatre.NewCluster(db, cfg)
	err := c2.Start(ctx)
	if err == nil {
		c2.Stop()
		t.Fatal("expected ErrHostIDConflict, got nil")
	}
	if err != theatre.ErrHostIDConflict {
		t.Fatalf("expected ErrHostIDConflict, got: %v", err)
	}
	t.Logf("correctly rejected duplicate host ID: %v", err)
}

func TestCluster_RemainingLease(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	if err := theatre.MigrateSchema(ctx, db); err != nil {
		t.Fatalf("MigrateSchema: %v", err)
	}
	cleanTestTables(t, db)

	cfg := theatre.ClusterConfig{
		HostID:        "lease-host",
		Address:       "127.0.0.1:9001",
		LeaseDuration: 10 * time.Second,
		PollInterval:  1 * time.Second,
	}

	c := theatre.NewCluster(db, cfg)
	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer c.Stop()

	rem := c.RemainingLease()
	// Should be close to 10s (within a couple seconds of registration).
	if rem < 7*time.Second || rem > 11*time.Second {
		t.Fatalf("remaining lease %s out of expected range [7s, 11s]", rem)
	}
	t.Logf("remaining lease after start: %s", rem.Round(time.Millisecond))

	// Wait a bit, remaining should decrease.
	time.Sleep(2 * time.Second)
	rem2 := c.RemainingLease()
	if rem2 >= rem {
		t.Fatalf("remaining lease did not decrease: %s → %s", rem, rem2)
	}
	t.Logf("remaining lease after 2s: %s", rem2.Round(time.Millisecond))
}

// --- helpers ---

func hostID(i int) string {
	return []string{"host-0", "host-1", "host-2"}[i]
}

func addr(i int) string {
	return []string{"127.0.0.1:7000", "127.0.0.1:7001", "127.0.0.1:7002"}[i]
}
