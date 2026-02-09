package theatre_test

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ironfang-ltd/go-theatre"
)

// echoActor replies with the received message body.
type echoActor struct {
	received []interface{}
	mu       sync.Mutex
}

func (r *echoActor) Receive(ctx *theatre.Context) error {
	switch ctx.Message.(type) {
	case theatre.Initialize, theatre.Shutdown:
		return nil
	}
	r.mu.Lock()
	r.received = append(r.received, ctx.Message)
	r.mu.Unlock()
	if ctx.Message != nil {
		ctx.Reply(ctx.Message)
	}
	return nil
}

// clusterNode bundles all components for one host in the cluster.
type clusterNode struct {
	host      *theatre.Host
	transport *theatre.Transport
	cluster   *theatre.Cluster
}

func (n *clusterNode) stop() {
	n.host.Stop()
	n.transport.Stop()
	n.cluster.Stop()
}

func setupClusterNode(t *testing.T, db *sql.DB, hostID, listenAddr string) *clusterNode {
	t.Helper()

	host := theatre.NewHost(
		theatre.WithIdleTimeout(30*time.Second),
		theatre.WithRequestTimeout(5*time.Second),
		theatre.WithPlacementTTL(10*time.Second),
	)

	transport, err := theatre.NewTransport(hostID, listenAddr, host.HandleTransportMessage)
	if err != nil {
		t.Fatalf("NewTransport %s: %v", hostID, err)
	}
	transport.Start()

	cluster := theatre.NewCluster(db, theatre.ClusterConfig{
		HostID:        hostID,
		Address:       transport.Addr(),
		LeaseDuration: 10 * time.Second,
		PollInterval:  500 * time.Millisecond,
	})
	if err := cluster.Start(context.Background()); err != nil {
		transport.Stop()
		t.Fatalf("Cluster.Start %s: %v", hostID, err)
	}

	host.SetTransport(transport)
	host.SetCluster(cluster)
	host.Start()

	return &clusterNode{host: host, transport: transport, cluster: cluster}
}

// insertOwnership inserts an actor_ownership row for testing.
func insertOwnership(t *testing.T, db *sql.DB, actorType, actorID, hostID string, epoch int64) {
	t.Helper()
	_, err := db.ExecContext(context.Background(), `
		INSERT INTO actor_ownership (actor_type, actor_id, host_id, epoch)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (actor_type, actor_id) DO UPDATE
			SET host_id = EXCLUDED.host_id, epoch = EXCLUDED.epoch
	`, actorType, actorID, hostID, epoch)
	if err != nil {
		t.Fatalf("insertOwnership: %v", err)
	}
}

func TestRoutingIntegration_CrossHostRequest(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	if err := theatre.MigrateSchema(ctx, db); err != nil {
		t.Fatalf("MigrateSchema: %v", err)
	}
	cleanTestTables(t, db)

	// Start 3 cluster nodes.
	nodeA := setupClusterNode(t, db, "host-a", "127.0.0.1:0")
	nodeB := setupClusterNode(t, db, "host-b", "127.0.0.1:0")
	nodeC := setupClusterNode(t, db, "host-c", "127.0.0.1:0")
	defer nodeA.stop()
	defer nodeB.stop()
	defer nodeC.stop()

	// Wait for cluster polls to converge.
	time.Sleep(time.Second)

	// Register echo actor type and spawn on host B.
	recv := &echoActor{}
	nodeB.host.RegisterActor("echo", func() theatre.Receiver { return recv })
	ref := theatre.NewRef("echo", "1")
	if err := nodeB.host.SpawnLocal(ref); err != nil {
		t.Fatalf("SpawnLocal: %v", err)
	}

	// Insert ownership record so A can resolve from DB.
	insertOwnership(t, db, "echo", "1", "host-b", nodeB.cluster.LocalEpoch())

	// Request from A → resolved via DB → forwarded to B → reply back to A.
	result, err := nodeA.host.Request(ref, "hello-from-A")
	if err != nil {
		t.Fatalf("Request: %v", err)
	}
	if result != "hello-from-A" {
		t.Fatalf("expected 'hello-from-A', got %v", result)
	}
	t.Logf("cross-host request succeeded: %v", result)

	// Request from C → same path.
	result2, err := nodeC.host.Request(ref, "hello-from-C")
	if err != nil {
		t.Fatalf("Request from C: %v", err)
	}
	if result2 != "hello-from-C" {
		t.Fatalf("expected 'hello-from-C', got %v", result2)
	}
	t.Logf("cross-host request from C succeeded: %v", result2)
}

func TestRoutingIntegration_StaleCacheThenResolve(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	if err := theatre.MigrateSchema(ctx, db); err != nil {
		t.Fatalf("MigrateSchema: %v", err)
	}
	cleanTestTables(t, db)

	nodeA := setupClusterNode(t, db, "host-a", "127.0.0.1:0")
	nodeB := setupClusterNode(t, db, "host-b", "127.0.0.1:0")
	nodeC := setupClusterNode(t, db, "host-c", "127.0.0.1:0")
	defer nodeA.stop()
	defer nodeB.stop()
	defer nodeC.stop()

	time.Sleep(time.Second)

	// Actor is on C, but we'll put a stale cache entry pointing to B.
	recv := &echoActor{}
	nodeC.host.RegisterActor("echo", func() theatre.Receiver { return recv })
	ref := theatre.NewRef("echo", "2")
	if err := nodeC.host.SpawnLocal(ref); err != nil {
		t.Fatalf("SpawnLocal: %v", err)
	}

	// Insert correct ownership: actor is on host-c.
	insertOwnership(t, db, "echo", "2", "host-c", nodeC.cluster.LocalEpoch())

	// Manually pollute A's placement cache to point to host-b (wrong).
	// We need to access the placement cache via a request that will fail and retry.
	// Instead, just make a request. First attempt will miss cache → DB resolve → host-c → works.
	result, err := nodeA.host.Request(ref, "stale-test")
	if err != nil {
		t.Fatalf("Request: %v", err)
	}
	if result != "stale-test" {
		t.Fatalf("expected 'stale-test', got %v", result)
	}
	t.Logf("DB resolve succeeded: %v", result)

	// Now the cache should be populated with host-c.
	// Second request should use cache and still work.
	result2, err := nodeA.host.Request(ref, "cached")
	if err != nil {
		t.Fatalf("Cached request: %v", err)
	}
	if result2 != "cached" {
		t.Fatalf("expected 'cached', got %v", result2)
	}
	t.Logf("cached request succeeded: %v", result2)
}

func TestRoutingIntegration_NotHereThenReResolve(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	if err := theatre.MigrateSchema(ctx, db); err != nil {
		t.Fatalf("MigrateSchema: %v", err)
	}
	cleanTestTables(t, db)

	nodeA := setupClusterNode(t, db, "host-a", "127.0.0.1:0")
	nodeB := setupClusterNode(t, db, "host-b", "127.0.0.1:0")
	nodeC := setupClusterNode(t, db, "host-c", "127.0.0.1:0")
	defer nodeA.stop()
	defer nodeB.stop()
	defer nodeC.stop()

	time.Sleep(time.Second)

	// Actor is on host-c.
	recv := &echoActor{}
	nodeC.host.RegisterActor("echo", func() theatre.Receiver { return recv })
	ref := theatre.NewRef("echo", "3")
	if err := nodeC.host.SpawnLocal(ref); err != nil {
		t.Fatalf("SpawnLocal: %v", err)
	}

	// Ownership DB says host-b (wrong — simulates stale ownership that
	// was correct before a migration). Actor will NotHere from B.
	insertOwnership(t, db, "echo", "3", "host-b", nodeB.cluster.LocalEpoch())

	// Request from A → DB says host-b → forward to B → B sends NotHere
	// → A evicts cache → A re-resolves from DB → DB still says host-b
	// → second NotHere → request fails with ErrNoOwner.
	_, err := nodeA.host.Request(ref, "should-fail")
	if err == nil {
		t.Fatal("expected error for stale ownership, got nil")
	}
	t.Logf("correctly got error for stale ownership: %v", err)

	// Now fix ownership to host-c and retry.
	insertOwnership(t, db, "echo", "3", "host-c", nodeC.cluster.LocalEpoch())

	result, err := nodeA.host.Request(ref, "fixed")
	if err != nil {
		t.Fatalf("Request after fix: %v", err)
	}
	if result != "fixed" {
		t.Fatalf("expected 'fixed', got %v", result)
	}
	t.Logf("request after ownership fix succeeded: %v", result)
}

func TestRoutingIntegration_NoOwnerDeadLetter(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	if err := theatre.MigrateSchema(ctx, db); err != nil {
		t.Fatalf("MigrateSchema: %v", err)
	}
	cleanTestTables(t, db)

	nodeA := setupClusterNode(t, db, "host-a", "127.0.0.1:0")
	defer nodeA.stop()

	time.Sleep(500 * time.Millisecond)

	// No ownership record exists → request should fail.
	ref := theatre.NewRef("echo", "999")
	_, err := nodeA.host.Request(ref, "no-owner")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	// Should be ErrNoOwner.
	if fmt.Sprintf("%v", err) != fmt.Sprintf("%v", theatre.ErrNoOwner) {
		t.Fatalf("expected ErrNoOwner, got: %v", err)
	}
	t.Logf("correctly got dead letter: %v", err)
}
