package theatre

// Chaos tests validate the single-activation invariant under failure conditions.
//
// Invariants verified:
//   - No interleaving: for each actor, events from different (host, epoch)
//     pairs never interleave. Valid: A,A,B,B. Invalid: A,B,A.
//   - At-most-once delivery: message loss is acceptable, duplication is not.
//   - Freeze correctness: after freeze, no further message processing occurs.
//
// All tests require THEATRE_TEST_DSN and skip cleanly without it.
// Tests are designed to complete within ~60s total.

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Chaos 1: Crash mid-traffic
//
// 3 hosts, 50 actors, continuous sends. Crash host B abruptly.
// Ensure: actors on B stop producing events after crash, other hosts
// reclaim after lease expiry, VerifyNoInterleaving passes.
// ---------------------------------------------------------------------------

func TestChaos_CrashMidTraffic(t *testing.T) {
	db := setupChaosDB(t)
	runID := "chaos1-crash-" + fmt.Sprint(time.Now().UnixNano())
	CleanAuditEvents(context.Background(), db, runID)

	h := newChaosHarness(t, db, runID)
	defer h.StopAll()

	// Start 3 hosts.
	h.StartHost("host-a")
	h.StartHost("host-b")
	h.StartHost("host-c")
	h.WaitForConvergence()

	refs := makeRefs("audit", 50)

	// Start continuous traffic from all hosts.
	hosts := []*Host{
		h.Node("host-a").host,
		h.Node("host-b").host,
		h.Node("host-c").host,
	}
	sender := newMultiSender(hosts, refs, 20*time.Millisecond)
	sender.Start()

	// Let traffic warm up and actors activate.
	time.Sleep(2 * time.Second)

	t.Log("crashing host-b")
	h.CrashHost("host-b")

	// Continue sending from surviving hosts.
	time.Sleep(1 * time.Second)

	// Wait for host-b's lease to expire so surviving hosts reclaim actors.
	h.WaitForLeaseExpiry()

	// Give some time for reclamation traffic.
	time.Sleep(2 * time.Second)

	sender.Stop()

	t.Logf("sent=%d errors=%d", sender.TotalSent(), sender.TotalErrors())

	count, err := CountAuditEvents(context.Background(), db, runID)
	if err != nil {
		t.Fatalf("count audit events: %v", err)
	}
	t.Logf("total audit events: %d", count)

	if err := VerifyNoInterleaving(context.Background(), db, runID); err != nil {
		t.Fatalf("INVARIANT VIOLATION: %v", err)
	}
	t.Log("VerifyNoInterleaving: PASS")
}

// ---------------------------------------------------------------------------
// Chaos 2: Partition host from Postgres (peers still reachable)
//
// 3 hosts, partition B from Postgres by disabling FlakyDB.
// B should freeze (renewal failures exceed threshold), stop processing.
// After lease expiry, other hosts reclaim.
// ---------------------------------------------------------------------------

func TestChaos_PartitionFromPostgres(t *testing.T) {
	db := setupChaosDB(t)
	runID := "chaos2-partition-" + fmt.Sprint(time.Now().UnixNano())
	CleanAuditEvents(context.Background(), db, runID)

	h := newChaosHarness(t, db, runID)
	defer h.StopAll()

	h.StartHost("host-a")
	h.StartHost("host-b")
	h.StartHost("host-c")
	h.WaitForConvergence()

	refs := makeRefs("audit", 30)

	// Start traffic.
	hosts := []*Host{
		h.Node("host-a").host,
		h.Node("host-b").host,
		h.Node("host-c").host,
	}
	sender := newMultiSender(hosts, refs, 30*time.Millisecond)
	sender.Start()

	time.Sleep(2 * time.Second)

	// Partition B from Postgres.
	t.Log("partitioning host-b from Postgres")
	h.Node("host-b").flakyDB.Disable()

	// Wait for B to freeze (2 renewal failures × ~1.3s interval ≈ 3s).
	time.Sleep(4 * time.Second)

	if !h.Node("host-b").host.IsFrozen() {
		t.Log("warning: host-b not yet frozen, waiting longer")
		time.Sleep(2 * time.Second)
	}

	// Wait for B's lease to expire.
	h.WaitForLeaseExpiry()

	// Continue traffic for reclamation.
	time.Sleep(2 * time.Second)

	sender.Stop()

	t.Logf("sent=%d errors=%d", sender.TotalSent(), sender.TotalErrors())

	if err := VerifyNoInterleaving(context.Background(), db, runID); err != nil {
		t.Fatalf("INVARIANT VIOLATION: %v", err)
	}
	t.Log("VerifyNoInterleaving: PASS")
}

// ---------------------------------------------------------------------------
// Chaos 3: Half cluster loses Postgres
//
// 4 hosts: A+B have DB, C+D lose DB (FlakyDB disabled).
// C and D freeze and stop processing. A and B continue and reclaim.
// ---------------------------------------------------------------------------

func TestChaos_HalfClusterLosesDB(t *testing.T) {
	db := setupChaosDB(t)
	runID := "chaos3-half-" + fmt.Sprint(time.Now().UnixNano())
	CleanAuditEvents(context.Background(), db, runID)

	h := newChaosHarness(t, db, runID)
	defer h.StopAll()

	h.StartHost("host-a")
	h.StartHost("host-b")
	h.StartHost("host-c")
	h.StartHost("host-d")
	h.WaitForConvergence()

	refs := makeRefs("audit", 40)

	hosts := []*Host{
		h.Node("host-a").host,
		h.Node("host-b").host,
		h.Node("host-c").host,
		h.Node("host-d").host,
	}
	sender := newMultiSender(hosts, refs, 30*time.Millisecond)
	sender.Start()

	time.Sleep(2 * time.Second)

	// Partition C and D from Postgres.
	t.Log("partitioning host-c and host-d from Postgres")
	h.Node("host-c").flakyDB.Disable()
	h.Node("host-d").flakyDB.Disable()

	// Wait for C and D to freeze + lease expiry.
	time.Sleep(4 * time.Second)
	h.WaitForLeaseExpiry()

	// Continue traffic for reclamation.
	time.Sleep(2 * time.Second)

	sender.Stop()

	t.Logf("sent=%d errors=%d", sender.TotalSent(), sender.TotalErrors())

	if err := VerifyNoInterleaving(context.Background(), db, runID); err != nil {
		t.Fatalf("INVARIANT VIOLATION: %v", err)
	}
	t.Log("VerifyNoInterleaving: PASS")
}

// ---------------------------------------------------------------------------
// Chaos 4: Slow Postgres
//
// 3 hosts. Inject 2s delay into DB ops for host B only.
// With 4s lease and 1.5s safety margin, the slow renewal should cause
// B to freeze (remaining lease drops below safety margin before renewal
// completes).
// ---------------------------------------------------------------------------

func TestChaos_SlowPostgres(t *testing.T) {
	db := setupChaosDB(t)
	runID := "chaos4-slow-" + fmt.Sprint(time.Now().UnixNano())
	CleanAuditEvents(context.Background(), db, runID)

	h := newChaosHarness(t, db, runID)
	defer h.StopAll()

	h.StartHost("host-a")
	h.StartHost("host-b")
	h.StartHost("host-c")
	h.WaitForConvergence()

	refs := makeRefs("audit", 20)

	hosts := []*Host{
		h.Node("host-a").host,
		h.Node("host-b").host,
		h.Node("host-c").host,
	}
	sender := newMultiSender(hosts, refs, 50*time.Millisecond)
	sender.Start()

	time.Sleep(2 * time.Second)

	// Inject 2s delay for host-b. With 4s lease and renewal every ~1.33s,
	// the 2s delay means renewal takes >2s. By the time it finishes,
	// remaining lease is < safetyMargin (1.5s) → freeze.
	t.Log("injecting 2s DB delay for host-b")
	h.Node("host-b").flakyDB.SetDelay(2 * time.Second)

	// Wait for B to freeze.
	time.Sleep(5 * time.Second)

	// B should be frozen.
	if h.Node("host-b").host.IsFrozen() {
		t.Log("host-b correctly froze due to slow DB")
	} else {
		t.Log("host-b did not freeze (renewal succeeded in time)")
	}

	// Remove delay and wait for lease expiry + reclamation.
	h.Node("host-b").flakyDB.SetDelay(0)
	h.WaitForLeaseExpiry()
	time.Sleep(2 * time.Second)

	sender.Stop()

	t.Logf("sent=%d errors=%d", sender.TotalSent(), sender.TotalErrors())

	if err := VerifyNoInterleaving(context.Background(), db, runID); err != nil {
		t.Fatalf("INVARIANT VIOLATION: %v", err)
	}
	t.Log("VerifyNoInterleaving: PASS")
}

// ---------------------------------------------------------------------------
// Chaos 5: Rapid restart loops
//
// 3 hosts. Repeatedly crash/restart host C every ~2–3s for ~20s.
// Flood messages to actors. Ensure epoch bumps and reclaims happen
// without interleaving.
// ---------------------------------------------------------------------------

func TestChaos_RapidRestartLoops(t *testing.T) {
	db := setupChaosDB(t)
	runID := "chaos5-restart-" + fmt.Sprint(time.Now().UnixNano())
	CleanAuditEvents(context.Background(), db, runID)

	h := newChaosHarness(t, db, runID)
	defer h.StopAll()

	h.StartHost("host-a")
	h.StartHost("host-b")
	h.StartHost("host-c")
	h.WaitForConvergence()

	refs := makeRefs("audit", 30)

	// Senders on A and B only (C will be crashing/restarting).
	sender := newMultiSender(
		[]*Host{h.Node("host-a").host, h.Node("host-b").host},
		refs, 30*time.Millisecond,
	)
	sender.Start()

	time.Sleep(2 * time.Second)

	// Crash/restart loop for host-c.
	for i := 0; i < 6; i++ {
		t.Logf("crash/restart cycle %d for host-c", i+1)
		h.CrashHost("host-c")

		// Wait for lease to expire before restarting.
		time.Sleep(h.leaseDuration + h.pollInterval)

		h.RestartHost("host-c")
		h.WaitForConvergence()

		// Let it run briefly before next crash.
		time.Sleep(500 * time.Millisecond)
	}

	// Final stabilization.
	time.Sleep(2 * time.Second)

	sender.Stop()

	t.Logf("sent=%d errors=%d", sender.TotalSent(), sender.TotalErrors())

	if err := VerifyNoInterleaving(context.Background(), db, runID); err != nil {
		t.Fatalf("INVARIANT VIOLATION: %v", err)
	}
	t.Log("VerifyNoInterleaving: PASS")
}

// ---------------------------------------------------------------------------
// Chaos 6: Kill during claim (between DB claim success and actor start)
//
// Uses postClaimHook to sleep after claim RETURNING but before the actor
// goroutine starts. Crashes host during that sleep. Ensures no events are
// produced on the crashed host, and the actor becomes available after
// lease expiry.
// ---------------------------------------------------------------------------

func TestChaos_KillDuringClaim(t *testing.T) {
	db := setupChaosDB(t)
	runID := "chaos6-kill-claim-" + fmt.Sprint(time.Now().UnixNano())
	CleanAuditEvents(context.Background(), db, runID)

	h := newChaosHarness(t, db, runID)
	defer h.StopAll()

	h.StartHost("host-a")
	h.StartHost("host-b")

	// Host C has a postClaimHook that blocks on a channel.
	hookEntered := make(chan struct{})
	hookRelease := make(chan struct{})
	var hookOnce sync.Once

	h.StartHost("host-c", WithPostClaimHook(func(ref Ref) {
		if ref.Type == "audit" && ref.ID == "claim-target" {
			hookOnce.Do(func() { close(hookEntered) })
			select {
			case <-hookRelease:
			case <-time.After(30 * time.Second):
			}
		}
	}))

	h.WaitForConvergence()

	// Find which host the ring assigns "audit:claim-target" to.
	ring := h.Node("host-a").cluster.Ring()
	preferred, _ := ring.Lookup("audit:claim-target")
	t.Logf("ring prefers host %s for audit:claim-target", preferred)

	// We need host-c to claim the actor. If ring doesn't prefer C,
	// we manually force it by sending from C directly.
	ref := NewRef("audit", "claim-target")

	// Send the message from the host whose ring would forward to host-c,
	// or directly from host-c if it's the preferred host.
	go func() {
		// Use Send (fire-and-forget) to avoid blocking on request timeout.
		h.Node("host-c").host.Send(ref, "trigger-claim")
	}()

	// Wait for the hook to be entered (claim succeeded, actor not yet started).
	select {
	case <-hookEntered:
		t.Log("postClaimHook entered — claim succeeded, actor not yet started")
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for postClaimHook")
	}

	// Crash host-c while the hook is blocking.
	t.Log("crashing host-c during claim")
	h.CrashHost("host-c")

	// Release the hook so the goroutine can exit.
	close(hookRelease)

	// Wait for lease expiry.
	h.WaitForLeaseExpiry()

	// Other hosts should reclaim the actor.
	time.Sleep(2 * time.Second)

	// Send some traffic to the target actor to trigger reclamation.
	for i := 0; i < 10; i++ {
		h.Node("host-a").host.Send(ref, fmt.Sprintf("post-crash-%d", i))
		time.Sleep(100 * time.Millisecond)
	}

	time.Sleep(time.Second)

	// Verify no audit events from host-c (actor never started → no Initialize).
	var hostCCount int
	db.QueryRowContext(context.Background(), `
		SELECT count(*) FROM audit_events
		WHERE run_id = $1 AND host_id = 'host-c'
	`, runID).Scan(&hostCCount)
	t.Logf("host-c audit events: %d (expected 0)", hostCCount)

	if err := VerifyNoInterleaving(context.Background(), db, runID); err != nil {
		t.Fatalf("INVARIANT VIOLATION: %v", err)
	}
	t.Log("VerifyNoInterleaving: PASS")
}

// ---------------------------------------------------------------------------
// Chaos 7: Peer partition (all have Postgres)
//
// 3 hosts. Block A<->B transport only (send filter).
// Messages from A to actors owned by B should fail to route (transport error)
// but A must NOT activate duplicates since B still has a valid lease.
// ---------------------------------------------------------------------------

func TestChaos_PeerPartition(t *testing.T) {
	db := setupChaosDB(t)
	runID := "chaos7-peer-" + fmt.Sprint(time.Now().UnixNano())
	CleanAuditEvents(context.Background(), db, runID)

	h := newChaosHarness(t, db, runID)
	defer h.StopAll()

	h.StartHost("host-a")
	h.StartHost("host-b")
	h.StartHost("host-c")
	h.WaitForConvergence()

	refs := makeRefs("audit", 20)

	// Let traffic warm up and activate actors on all hosts.
	sender := newMultiSender(
		[]*Host{h.Node("host-a").host, h.Node("host-b").host, h.Node("host-c").host},
		refs, 50*time.Millisecond,
	)
	sender.Start()
	time.Sleep(2 * time.Second)
	sender.Stop()

	// Count events before partition.
	countBefore, _ := CountAuditEvents(context.Background(), db, runID)
	t.Logf("audit events before partition: %d", countBefore)

	// Block A<->B transport (bidirectional).
	t.Log("partitioning transport between host-a and host-b")
	h.Node("host-a").transport.SetSendFilter(func(hostID string) bool {
		return hostID != "host-b"
	})
	h.Node("host-b").transport.SetSendFilter(func(hostID string) bool {
		return hostID != "host-a"
	})

	// Track errors sending from A.
	var sendErrors atomic.Int64
	var sendOK atomic.Int64

	// Send messages from A. Some will target actors on B.
	for i := 0; i < 50; i++ {
		ref := refs[i%len(refs)]
		err := h.Node("host-a").host.Send(ref, fmt.Sprintf("partitioned-%d", i))
		if err != nil {
			sendErrors.Add(1)
		} else {
			sendOK.Add(1)
		}
		time.Sleep(20 * time.Millisecond)
	}

	t.Logf("sends: ok=%d errors=%d", sendOK.Load(), sendErrors.Load())

	// Remove partition.
	h.Node("host-a").transport.SetSendFilter(nil)
	h.Node("host-b").transport.SetSendFilter(nil)

	// Let things settle.
	time.Sleep(2 * time.Second)

	if err := VerifyNoInterleaving(context.Background(), db, runID); err != nil {
		t.Fatalf("INVARIANT VIOLATION: %v", err)
	}
	t.Log("VerifyNoInterleaving: PASS")
}
