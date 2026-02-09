package theatre

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// ---------------------------------------------------------------------------
// FlakyDB — wraps *sql.DB to simulate Postgres partitions and latency.
// Implements SQLDB so it can be injected into Cluster.
// ---------------------------------------------------------------------------

type FlakyDB struct {
	inner    *sql.DB
	disabled atomic.Bool  // when true, all operations fail
	delayMs  atomic.Int64 // artificial delay before each operation (ms)
}

func NewFlakyDB(inner *sql.DB) *FlakyDB {
	return &FlakyDB{inner: inner}
}

func (f *FlakyDB) Disable() { f.disabled.Store(true) }
func (f *FlakyDB) Enable()  { f.disabled.Store(false) }

func (f *FlakyDB) SetDelay(d time.Duration) {
	f.delayMs.Store(d.Milliseconds())
}

func (f *FlakyDB) applyDelay() {
	if ms := f.delayMs.Load(); ms > 0 {
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (f *FlakyDB) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	if f.disabled.Load() {
		// Return a Row whose Scan returns context.Canceled.
		ctx, cancel := context.WithCancel(ctx)
		cancel()
		return f.inner.QueryRowContext(ctx, query, args...)
	}
	f.applyDelay()
	return f.inner.QueryRowContext(ctx, query, args...)
}

func (f *FlakyDB) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if f.disabled.Load() {
		return nil, fmt.Errorf("simulated: DB connection failed")
	}
	f.applyDelay()
	return f.inner.QueryContext(ctx, query, args...)
}

func (f *FlakyDB) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if f.disabled.Load() {
		return nil, fmt.Errorf("simulated: DB connection failed")
	}
	f.applyDelay()
	return f.inner.ExecContext(ctx, query, args...)
}

func (f *FlakyDB) Conn(ctx context.Context) (*sql.Conn, error) {
	if f.disabled.Load() {
		return nil, fmt.Errorf("simulated: DB connection failed")
	}
	return f.inner.Conn(ctx)
}

// ---------------------------------------------------------------------------
// chaosNode — bundles all components for one host in the chaos harness.
// ---------------------------------------------------------------------------

type chaosNode struct {
	host      *Host
	transport *Transport
	cluster   *Cluster
	flakyDB   *FlakyDB
	hostID    string
	crashed   bool
}

func (n *chaosNode) stop() {
	if n.crashed {
		// Only stop host goroutines (transport and cluster already torn down).
		n.host.Stop()
		return
	}
	n.host.Stop()
	n.transport.Stop()
	n.cluster.Stop()
}

// ---------------------------------------------------------------------------
// chaosHarness — manages N hosts in-process for chaos testing.
//
// Invariants verified:
//   - Single activation: no two hosts process messages for the same actor
//     concurrently (verified via VerifyNoInterleaving).
//   - Freeze correctness: after freeze, no further message processing
//     occurs on the frozen host.
// ---------------------------------------------------------------------------

type chaosHarness struct {
	t       *testing.T
	realDB  *sql.DB
	runID   string
	nodes   map[string]*chaosNode
	mu      sync.Mutex
	stopped bool

	// Shared configuration for all nodes.
	leaseDuration  time.Duration
	pollInterval   time.Duration
	safetyMargin   time.Duration
	freezeGrace    time.Duration
	maxFailures    int
	requestTimeout time.Duration
	idleTimeout    time.Duration
}

func newChaosHarness(t *testing.T, db *sql.DB, runID string) *chaosHarness {
	t.Helper()
	return &chaosHarness{
		t:              t,
		realDB:         db,
		runID:          runID,
		nodes:          make(map[string]*chaosNode),
		leaseDuration:  4 * time.Second,
		pollInterval:   300 * time.Millisecond,
		safetyMargin:   1500 * time.Millisecond,
		freezeGrace:    500 * time.Millisecond,
		maxFailures:    2,
		requestTimeout: 3 * time.Second,
		idleTimeout:    120 * time.Second,
	}
}

// StartHost creates and starts a new host with the given ID.
func (h *chaosHarness) StartHost(hostID string, opts ...Option) *chaosNode {
	h.t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()

	fdb := NewFlakyDB(h.realDB)

	baseOpts := []Option{
		WithIdleTimeout(h.idleTimeout),
		WithRequestTimeout(h.requestTimeout),
		WithPlacementTTL(2 * time.Second),
		WithFreezeGracePeriod(h.freezeGrace),
		WithSafetyMargin(h.safetyMargin),
		WithMaxRenewalFailures(h.maxFailures),
	}
	baseOpts = append(baseOpts, opts...)

	host := NewHost(baseOpts...)

	transport, err := NewTransport(hostID, "127.0.0.1:0", host.HandleTransportMessage)
	if err != nil {
		h.t.Fatalf("NewTransport %s: %v", hostID, err)
	}
	transport.Start()

	cluster := NewCluster(fdb, ClusterConfig{
		HostID:        hostID,
		Address:       transport.Addr(),
		LeaseDuration: h.leaseDuration,
		PollInterval:  h.pollInterval,
	})
	if err := cluster.Start(context.Background()); err != nil {
		transport.Stop()
		h.t.Fatalf("Cluster.Start %s: %v", hostID, err)
	}

	host.SetTransport(transport)
	host.SetCluster(cluster)

	// Register audit actor type.
	host.RegisterActor("audit", func() Receiver {
		return &auditActor{
			db:      h.realDB,
			runID:   h.runID,
			cluster: cluster,
		}
	})

	host.Start()

	node := &chaosNode{
		host:      host,
		transport: transport,
		cluster:   cluster,
		flakyDB:   fdb,
		hostID:    hostID,
	}
	h.nodes[hostID] = node
	return node
}

// CrashHost simulates an abrupt host crash:
//   - Disables DB to prevent ownership release during cleanup.
//   - Cancels all actor contexts (actors exit without graceful shutdown).
//   - Clears the actor registry.
//   - Closes transport (listener + peer connections).
//   - Stops cluster goroutines (renewal stops → lease expires naturally).
//   - Closes advisory connection (releases PG lock for restart).
//
// The host's lease remains valid in Postgres until it expires naturally,
// simulating a real process crash.
func (h *chaosHarness) CrashHost(hostID string) {
	h.mu.Lock()
	node := h.nodes[hostID]
	h.mu.Unlock()
	if node == nil || node.crashed {
		return
	}

	// 1. Disable FlakyDB to prevent onDeactivate from releasing ownership rows.
	node.flakyDB.Disable()

	// 2. Set frozen flag so processInbox/processOutbox stop processing.
	node.host.frozen.Store(true)

	// 3. Cancel freeze context → all actor contexts cancelled → actors exit.
	node.host.freezeCancel()

	// 4. Fail all pending requests.
	node.host.requests.FailAll(ErrHostFrozen)

	// 5. Clear actor registry (no Shutdown messages — simulates crash).
	node.host.actors.ForceDeregisterAll()

	// 6. Close transport (listener + peer connections).
	node.transport.Stop()

	// 7. Stop cluster (renewal/poll goroutines + advisory lock).
	node.cluster.Stop()

	node.crashed = true
}

// RestartHost restarts a previously crashed host with the same host ID.
// A new epoch is assigned automatically by Cluster.Start.
func (h *chaosHarness) RestartHost(hostID string, opts ...Option) *chaosNode {
	h.t.Helper()
	h.mu.Lock()
	old := h.nodes[hostID]
	h.mu.Unlock()

	// If the old node is still alive, stop it gracefully first.
	if old != nil && !old.crashed {
		old.stop()
	}

	// If old node was crashed, ensure host goroutines are stopped.
	if old != nil && old.crashed {
		old.host.Stop()
	}

	h.mu.Lock()
	delete(h.nodes, hostID)
	h.mu.Unlock()

	return h.StartHost(hostID, opts...)
}

// StopAll gracefully stops all hosts. Call this in test cleanup.
func (h *chaosHarness) StopAll() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.stopped {
		return
	}
	h.stopped = true
	for _, node := range h.nodes {
		node.stop()
	}
}

// WaitForConvergence waits for cluster polls to converge across all nodes.
func (h *chaosHarness) WaitForConvergence() {
	time.Sleep(2 * h.pollInterval)
}

// WaitForLeaseExpiry waits long enough for a crashed/partitioned host's
// lease to expire and for surviving hosts to detect the change.
func (h *chaosHarness) WaitForLeaseExpiry() {
	time.Sleep(h.leaseDuration + 2*h.pollInterval)
}

// Node returns the chaosNode for the given hostID.
func (h *chaosHarness) Node(hostID string) *chaosNode {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.nodes[hostID]
}

// ---------------------------------------------------------------------------
// Test DB helper (mirrors the one in cluster_integration_test.go but for
// package theatre internal tests).
// ---------------------------------------------------------------------------

func openChaosDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("THEATRE_TEST_DSN")
	if dsn == "" {
		t.Skip("THEATRE_TEST_DSN not set — skipping chaos test")
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

func setupChaosDB(t *testing.T) *sql.DB {
	t.Helper()
	db := openChaosDB(t)
	ctx := context.Background()
	if err := MigrateSchema(ctx, db); err != nil {
		t.Fatalf("MigrateSchema: %v", err)
	}
	if err := CreateAuditSchema(ctx, db); err != nil {
		t.Fatalf("CreateAuditSchema: %v", err)
	}
	// Clean tables.
	db.ExecContext(ctx, `DELETE FROM actor_ownership`)
	db.ExecContext(ctx, `DELETE FROM hosts`)
	return db
}

// ---------------------------------------------------------------------------
// Message sender — sends continuous traffic to a set of actors.
// ---------------------------------------------------------------------------

type messageSender struct {
	host     *Host
	refs     []Ref
	interval time.Duration
	done     chan struct{}
	wg       sync.WaitGroup
	errors   atomic.Int64
	sent     atomic.Int64
}

func newMessageSender(host *Host, refs []Ref, interval time.Duration) *messageSender {
	return &messageSender{
		host:     host,
		refs:     refs,
		interval: interval,
		done:     make(chan struct{}),
	}
}

func (s *messageSender) Start() {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ticker := time.NewTicker(s.interval)
		defer ticker.Stop()
		i := 0
		for {
			select {
			case <-s.done:
				return
			case <-ticker.C:
				ref := s.refs[i%len(s.refs)]
				i++
				err := s.host.Send(ref, fmt.Sprintf("msg-%d", i))
				if err != nil {
					s.errors.Add(1)
				} else {
					s.sent.Add(1)
				}
			}
		}
	}()
}

func (s *messageSender) Stop() {
	close(s.done)
	s.wg.Wait()
}

// multiSender sends messages from multiple hosts concurrently.
type multiSender struct {
	senders []*messageSender
}

func newMultiSender(hosts []*Host, refs []Ref, interval time.Duration) *multiSender {
	ms := &multiSender{}
	for _, h := range hosts {
		ms.senders = append(ms.senders, newMessageSender(h, refs, interval))
	}
	return ms
}

func (ms *multiSender) Start() {
	for _, s := range ms.senders {
		s.Start()
	}
}

func (ms *multiSender) Stop() {
	for _, s := range ms.senders {
		s.Stop()
	}
}

func (ms *multiSender) TotalSent() int64 {
	var total int64
	for _, s := range ms.senders {
		total += s.sent.Load()
	}
	return total
}

func (ms *multiSender) TotalErrors() int64 {
	var total int64
	for _, s := range ms.senders {
		total += s.errors.Load()
	}
	return total
}

// makeRefs creates a slice of Refs with the given type and numbered IDs.
func makeRefs(actorType string, count int) []Ref {
	refs := make([]Ref, count)
	for i := range refs {
		refs[i] = NewRef(actorType, fmt.Sprintf("%d", i))
	}
	return refs
}
