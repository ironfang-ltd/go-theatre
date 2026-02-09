package theatre_test

import (
	"context"
	"database/sql"
	"sync"
	"testing"
	"time"

	"github.com/ironfang-ltd/go-theatre"
)

// reasonTracker records the ActivationReason from Initialize.
type reasonTracker struct {
	mu     sync.Mutex
	reason theatre.ActivationReason
	set    bool
	msgs   []interface{}
}

func (r *reasonTracker) Receive(ctx *theatre.Context) error {
	switch v := ctx.Message.(type) {
	case theatre.Initialize:
		r.mu.Lock()
		r.reason = v.Reason
		r.set = true
		r.mu.Unlock()
		return nil
	case theatre.Shutdown:
		return nil
	}
	r.mu.Lock()
	r.msgs = append(r.msgs, ctx.Message)
	r.mu.Unlock()
	if ctx.Message != nil {
		ctx.Reply(ctx.Message)
	}
	return nil
}

func (r *reasonTracker) getReason() (theatre.ActivationReason, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.reason, r.set
}

func (r *reasonTracker) getMessages() []interface{} {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]interface{}, len(r.msgs))
	copy(out, r.msgs)
	return out
}

// deleteOwnership removes the actor_ownership row for testing.
func deleteOwnership(t *testing.T, db *sql.DB, actorType, actorID string) {
	t.Helper()
	_, err := db.ExecContext(context.Background(), `
		DELETE FROM actor_ownership WHERE actor_type = $1 AND actor_id = $2
	`, actorType, actorID)
	if err != nil {
		t.Fatalf("deleteOwnership: %v", err)
	}
}

// getOwnership reads the current owner from actor_ownership.
func getOwnership(t *testing.T, db *sql.DB, actorType, actorID string) (hostID string, epoch int64, found bool) {
	t.Helper()
	err := db.QueryRowContext(context.Background(), `
		SELECT host_id, epoch FROM actor_ownership
		WHERE actor_type = $1 AND actor_id = $2
	`, actorType, actorID).Scan(&hostID, &epoch)
	if err == sql.ErrNoRows {
		return "", 0, false
	}
	if err != nil {
		t.Fatalf("getOwnership: %v", err)
	}
	return hostID, epoch, true
}

func TestActivationIntegration_ClaimNewActor(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	if err := theatre.MigrateSchema(ctx, db); err != nil {
		t.Fatalf("MigrateSchema: %v", err)
	}
	cleanTestTables(t, db)

	nodeA := setupClusterNode(t, db, "host-a", "127.0.0.1:0")
	defer nodeA.stop()

	time.Sleep(time.Second)

	// Register actor type.
	tracker := &reasonTracker{}
	nodeA.host.RegisterActor("claim-test", func() theatre.Receiver { return tracker })

	ref := theatre.NewRef("claim-test", "new-1")

	// Request should trigger: no cache → no DB owner → ring → claim → activate.
	result, err := nodeA.host.Request(ref, "hello")
	if err != nil {
		t.Fatalf("Request: %v", err)
	}
	if result != "hello" {
		t.Fatalf("expected 'hello', got %v", result)
	}

	// Verify ownership was claimed.
	ownerHost, ownerEpoch, found := getOwnership(t, db, "claim-test", "new-1")
	if !found {
		t.Fatal("ownership not found")
	}
	if ownerHost != "host-a" {
		t.Fatalf("expected owner host-a, got %s", ownerHost)
	}
	if ownerEpoch != nodeA.cluster.LocalEpoch() {
		t.Fatalf("expected epoch %d, got %d", nodeA.cluster.LocalEpoch(), ownerEpoch)
	}
	t.Logf("claimed ownership: host=%s epoch=%d", ownerHost, ownerEpoch)

	// Verify activation reason.
	reason, set := tracker.getReason()
	if !set {
		t.Fatal("Initialize not received")
	}
	if reason != theatre.ActivationNew {
		t.Fatalf("expected ActivationNew, got %d", reason)
	}
}

func TestActivationIntegration_ClaimCollision(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	if err := theatre.MigrateSchema(ctx, db); err != nil {
		t.Fatalf("MigrateSchema: %v", err)
	}
	cleanTestTables(t, db)

	nodeA := setupClusterNode(t, db, "host-a", "127.0.0.1:0")
	nodeB := setupClusterNode(t, db, "host-b", "127.0.0.1:0")
	defer nodeA.stop()
	defer nodeB.stop()

	time.Sleep(time.Second)

	// Register on both nodes.
	trackerA := &reasonTracker{}
	trackerB := &reasonTracker{}
	nodeA.host.RegisterActor("race", func() theatre.Receiver { return trackerA })
	nodeB.host.RegisterActor("race", func() theatre.Receiver { return trackerB })

	ref := theatre.NewRef("race", "collision-1")

	// Pre-insert ownership for host-a with valid epoch.
	insertOwnership(t, db, "race", "collision-1", "host-a", nodeA.cluster.LocalEpoch())

	// Spawn on host-a so it's actually running there.
	if err := nodeA.host.SpawnLocal(ref); err != nil {
		t.Fatal(err)
	}

	// Request from host-b. Since host-a holds valid ownership,
	// host-b's attempt to claim should lose. The message should
	// route to host-a via DB resolve and succeed.
	result, err := nodeB.host.Request(ref, "from-B")
	if err != nil {
		t.Fatalf("Request from B: %v", err)
	}
	if result != "from-B" {
		t.Fatalf("expected 'from-B', got %v", result)
	}
	t.Logf("collision test passed: request routed to host-a")

	// Verify ownership didn't change.
	ownerHost, _, found := getOwnership(t, db, "race", "collision-1")
	if !found {
		t.Fatal("ownership not found")
	}
	if ownerHost != "host-a" {
		t.Fatalf("expected owner host-a, got %s", ownerHost)
	}
}

func TestActivationIntegration_FailoverReason(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	if err := theatre.MigrateSchema(ctx, db); err != nil {
		t.Fatalf("MigrateSchema: %v", err)
	}
	cleanTestTables(t, db)

	nodeA := setupClusterNode(t, db, "host-a", "127.0.0.1:0")
	nodeB := setupClusterNode(t, db, "host-b", "127.0.0.1:0")
	defer nodeA.stop()
	defer nodeB.stop()

	time.Sleep(time.Second)

	tracker := &reasonTracker{}
	nodeA.host.RegisterActor("failover", func() theatre.Receiver { return tracker })
	nodeB.host.RegisterActor("failover", func() theatre.Receiver { return &echoActor{} })

	ref := theatre.NewRef("failover", "fo-1")

	// Insert stale ownership for host-b with an old epoch (not matching
	// host-b's current epoch). This simulates a previous host that crashed.
	insertOwnership(t, db, "failover", "fo-1", "host-b", 0)

	// Request from host-a. DB resolveOwner returns nil (epoch mismatch).
	// Ring picks a preferred host. If it picks host-a, host-a claims and activates.
	// The claim should see the old host-b ownership → ActivationFailover.
	result, err := nodeA.host.Request(ref, "failover-msg")
	if err != nil {
		t.Fatalf("Request: %v", err)
	}
	if result != "failover-msg" {
		t.Fatalf("expected 'failover-msg', got %v", result)
	}

	// Verify new ownership.
	ownerHost, _, found := getOwnership(t, db, "failover", "fo-1")
	if !found {
		t.Fatal("ownership not found")
	}
	t.Logf("failover: new owner is %s", ownerHost)

	// Check the reason. We may not know which host got it (depends on ring),
	// but the reason should be Failover since there was a previous owner.
	time.Sleep(100 * time.Millisecond)
	if ownerHost == "host-a" {
		reason, set := tracker.getReason()
		if !set {
			t.Fatal("Initialize not received on host-a")
		}
		if reason != theatre.ActivationFailover {
			t.Fatalf("expected ActivationFailover, got %d", reason)
		}
		t.Logf("failover reason confirmed on host-a")
	} else {
		t.Logf("actor activated on %s (ring preference), skipping reason check on host-a", ownerHost)
	}
}

func TestActivationIntegration_CrossHostActivation(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	if err := theatre.MigrateSchema(ctx, db); err != nil {
		t.Fatalf("MigrateSchema: %v", err)
	}
	cleanTestTables(t, db)

	nodeA := setupClusterNode(t, db, "host-a", "127.0.0.1:0")
	nodeB := setupClusterNode(t, db, "host-b", "127.0.0.1:0")
	defer nodeA.stop()
	defer nodeB.stop()

	time.Sleep(time.Second)

	// Register on both hosts.
	nodeA.host.RegisterActor("cross", func() theatre.Receiver { return &echoActor{} })
	nodeB.host.RegisterActor("cross", func() theatre.Receiver { return &echoActor{} })

	// Send a request. Since no ownership exists, the ring determines the preferred host.
	// The actor should be activated on whichever host the ring picks.
	ref := theatre.NewRef("cross", "auto-1")
	result, err := nodeA.host.Request(ref, "auto-activate")
	if err != nil {
		t.Fatalf("Request: %v", err)
	}
	if result != "auto-activate" {
		t.Fatalf("expected 'auto-activate', got %v", result)
	}

	// Verify ownership was established.
	ownerHost, _, found := getOwnership(t, db, "cross", "auto-1")
	if !found {
		t.Fatal("ownership not found after activation")
	}
	t.Logf("auto-activated on %s", ownerHost)

	// Second request should use cache and succeed.
	result2, err := nodeA.host.Request(ref, "cached")
	if err != nil {
		t.Fatalf("Cached request: %v", err)
	}
	if result2 != "cached" {
		t.Fatalf("expected 'cached', got %v", result2)
	}
}

func TestActivationIntegration_DeactivationReleasesOwnership(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	if err := theatre.MigrateSchema(ctx, db); err != nil {
		t.Fatalf("MigrateSchema: %v", err)
	}
	cleanTestTables(t, db)

	nodeA := setupClusterNode(t, db, "host-a", "127.0.0.1:0")
	defer nodeA.stop()

	time.Sleep(time.Second)

	nodeA.host.RegisterActor("deact", func() theatre.Receiver { return &echoActor{} })

	ref := theatre.NewRef("deact", "release-1")

	// Activate actor via request.
	result, err := nodeA.host.Request(ref, "activate")
	if err != nil {
		t.Fatalf("Request: %v", err)
	}
	if result != "activate" {
		t.Fatalf("expected 'activate', got %v", result)
	}

	// Verify ownership exists.
	_, _, found := getOwnership(t, db, "deact", "release-1")
	if !found {
		t.Fatal("ownership not found after activation")
	}

	// Stop the host — this triggers actor shutdown → onDeactivate → releaseOwnership.
	nodeA.stop()

	// Give DB a moment.
	time.Sleep(200 * time.Millisecond)

	// Ownership should be released.
	_, _, found = getOwnership(t, db, "deact", "release-1")
	if found {
		t.Fatal("ownership still exists after deactivation")
	}
	t.Logf("ownership correctly released after host stop")
}

func TestActivationIntegration_ConcurrentRequestsSameActor(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	if err := theatre.MigrateSchema(ctx, db); err != nil {
		t.Fatalf("MigrateSchema: %v", err)
	}
	cleanTestTables(t, db)

	nodeA := setupClusterNode(t, db, "host-a", "127.0.0.1:0")
	defer nodeA.stop()

	time.Sleep(time.Second)

	nodeA.host.RegisterActor("concurrent", func() theatre.Receiver { return &echoActor{} })

	ref := theatre.NewRef("concurrent", "race-1")

	// Fire N concurrent requests for the same actor.
	const N = 10
	var wg sync.WaitGroup
	results := make([]interface{}, N)
	errs := make([]error, N)

	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			results[idx], errs[idx] = nodeA.host.Request(ref, idx)
		}(i)
	}
	wg.Wait()

	// All should succeed.
	for i := 0; i < N; i++ {
		if errs[i] != nil {
			t.Fatalf("request %d: %v", i, errs[i])
		}
		if results[i] != i {
			t.Fatalf("request %d: expected %d, got %v", i, i, results[i])
		}
	}

	// Only one ownership row should exist.
	ownerHost, _, found := getOwnership(t, db, "concurrent", "race-1")
	if !found {
		t.Fatal("ownership not found")
	}
	if ownerHost != "host-a" {
		t.Fatalf("expected host-a, got %s", ownerHost)
	}
	t.Logf("concurrent activation: all %d requests succeeded, owner=%s", N, ownerHost)
}
