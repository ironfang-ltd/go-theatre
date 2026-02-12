package theatre

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// ErrHostIDConflict is returned by Cluster.Start when another process
// already holds the advisory lock for the same host ID.
var ErrHostIDConflict = fmt.Errorf("host ID conflict: another process holds this identity")

// ClusterConfig configures a Cluster node.
type ClusterConfig struct {
	HostID  string
	Address string

	// LeaseDuration is the Postgres-side lease length. Default 20s.
	LeaseDuration time.Duration
	// PollInterval controls how often the host table is polled. Default 3s.
	PollInterval time.Duration
}

func (c *ClusterConfig) applyDefaults() {
	if c.LeaseDuration == 0 {
		c.LeaseDuration = 20 * time.Second
	}
	if c.PollInterval == 0 {
		c.PollInterval = 3 * time.Second
	}
}

// HostInfo describes a live host as seen by the polling query.
type HostInfo struct {
	HostID      string
	Address     string // transport address
	AdminAddr   string // admin HTTP address (optional)
	Epoch       int64
	LeaseExpiry time.Time
}

// SQLDB abstracts database operations for testability. *sql.DB satisfies
// this interface natively. Test code can inject wrappers (e.g. FlakyDB)
// to simulate Postgres partitions or latency.
type SQLDB interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	Conn(ctx context.Context) (*sql.Conn, error)
}

// Cluster manages this host's membership in a Postgres-backed cluster.
// It acquires an advisory lock on the host ID, registers with an epoch bump,
// renews its lease periodically, polls for live hosts, and maintains a
// consistent hash ring.
// hostsSnapshot is an immutable snapshot of live hosts, swapped atomically.
type hostsSnapshot struct {
	list []HostInfo
	byID map[string]HostInfo // O(1) lookup by host ID
}

type Cluster struct {
	db     SQLDB
	config ClusterConfig

	mu        sync.RWMutex
	epoch     int64
	pgExpiry  time.Time // lease_expiry from Postgres
	pgNow     time.Time // now() from Postgres at last renewal
	localBase time.Time // time.Now() at last renewal / registration
	hosts     []HostInfo

	// Lock-free snapshot for hot-path reads (LiveHosts, HostLookup).
	hostsSnap atomic.Pointer[hostsSnapshot]

	ring         *HashRing
	advisoryConn *sql.Conn

	renewalFailures int64 // atomic: consecutive renewal failures

	done     chan struct{}
	wg       sync.WaitGroup
	stopOnce sync.Once
}

// NewCluster creates a Cluster but does not start it.
func NewCluster(db SQLDB, config ClusterConfig) *Cluster {
	config.applyDefaults()
	return &Cluster{
		db:     db,
		config: config,
		ring:   NewHashRing(),
		done:   make(chan struct{}),
	}
}

// NewRingOnlyCluster creates a Cluster with no database connection.
// It uses only a hash ring for deterministic actor placement.
// No Start() call is needed — there are no background goroutines.
// Stop() is safe to call (closes done channel, no-ops on empty WaitGroup).
func NewRingOnlyCluster(hostID string, address string, epoch int64) *Cluster {
	c := &Cluster{
		config: ClusterConfig{HostID: hostID, Address: address},
		ring:   NewHashRing(),
		done:   make(chan struct{}),
	}
	c.epoch = epoch
	return c
}

// SetHosts updates the live host list. Used by ring-only clusters where
// hosts are known statically rather than discovered via polling.
func (c *Cluster) SetHosts(hosts []HostInfo) {
	c.mu.Lock()
	c.hosts = hosts
	c.mu.Unlock()
	c.storeHostsSnapshot(hosts)
}

// storeHostsSnapshot builds an immutable snapshot and stores it atomically.
func (c *Cluster) storeHostsSnapshot(hosts []HostInfo) {
	snap := &hostsSnapshot{
		list: hosts,
		byID: make(map[string]HostInfo, len(hosts)),
	}
	for _, h := range hosts {
		snap.byID[h.HostID] = h
	}
	c.hostsSnap.Store(snap)
}

// Start acquires the advisory lock, registers this host (bumping the epoch),
// performs an initial host poll + ring build, and launches background
// goroutines for lease renewal and polling.
func (c *Cluster) Start(ctx context.Context) error {
	if err := c.acquireAdvisoryLock(ctx); err != nil {
		return err
	}

	if err := c.register(ctx); err != nil {
		c.releaseAdvisoryLock()
		return fmt.Errorf("cluster register: %w", err)
	}

	if err := c.pollHosts(ctx); err != nil {
		c.releaseAdvisoryLock()
		return fmt.Errorf("cluster initial poll: %w", err)
	}

	c.wg.Add(2)
	go c.renewLoop()
	go c.pollLoop()

	slog.Info("cluster started",
		"hostID", c.config.HostID,
		"epoch", c.LocalEpoch(),
		"lease", c.RemainingLease().Round(time.Millisecond))

	return nil
}

// Stop signals background goroutines, waits for them, and releases
// the advisory lock. Safe to call multiple times (idempotent via sync.Once).
func (c *Cluster) Stop() {
	c.stopOnce.Do(func() {
		close(c.done)
		c.wg.Wait()
		c.releaseAdvisoryLock()

		slog.Info("cluster stopped", "hostID", c.config.HostID)
	})
}

// DB returns the underlying database handle for ownership queries.
func (c *Cluster) DB() SQLDB { return c.db }

// Ring returns the consistent hash ring.
func (c *Cluster) Ring() *HashRing { return c.ring }

// LiveHosts returns a snapshot of the last polled live hosts.
// The returned slice is shared and must not be modified.
func (c *Cluster) LiveHosts() []HostInfo {
	if snap := c.hostsSnap.Load(); snap != nil {
		return snap.list
	}
	return nil
}

// HostLookup returns the HostInfo for a given host ID. O(1) lookup.
func (c *Cluster) HostLookup(hostID string) (HostInfo, bool) {
	if snap := c.hostsSnap.Load(); snap != nil {
		h, ok := snap.byID[hostID]
		return h, ok
	}
	return HostInfo{}, false
}

// LocalHostID returns this host's configured ID.
func (c *Cluster) LocalHostID() string { return c.config.HostID }

// LocalEpoch returns the epoch assigned at registration.
func (c *Cluster) LocalEpoch() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.epoch
}

// RemainingLease returns the estimated time until the lease expires,
// computed from the Postgres lease_expiry and monotonic local time
// to avoid wall-clock drift.
func (c *Cluster) RemainingLease() time.Duration {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// leaseLen is the PG-side remaining lease at the moment we last renewed.
	leaseLen := c.pgExpiry.Sub(c.pgNow)
	// elapsed is how much monotonic time has passed since that moment.
	elapsed := time.Since(c.localBase)
	remaining := leaseLen - elapsed
	if remaining < 0 {
		return 0
	}
	return remaining
}

// --- advisory lock ---

func (c *Cluster) acquireAdvisoryLock(ctx context.Context) error {
	conn, err := c.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("cluster advisory conn: %w", err)
	}

	key := advisoryLockKey(c.config.HostID)
	var acquired bool
	if err := conn.QueryRowContext(ctx,
		`SELECT pg_try_advisory_lock($1)`, key).Scan(&acquired); err != nil {
		conn.Close()
		return fmt.Errorf("cluster advisory lock: %w", err)
	}
	if !acquired {
		conn.Close()
		return ErrHostIDConflict
	}

	c.advisoryConn = conn
	return nil
}

func (c *Cluster) releaseAdvisoryLock() {
	if c.advisoryConn == nil {
		return
	}
	key := advisoryLockKey(c.config.HostID)
	// Best-effort unlock; connection close also releases session locks.
	c.advisoryConn.ExecContext(context.Background(),
		`SELECT pg_advisory_unlock($1)`, key)
	c.advisoryConn.Close()
	c.advisoryConn = nil
}

func advisoryLockKey(hostID string) int64 {
	return int64(fnvHash64(hostID))
}

// --- registration + epoch bump ---

func (c *Cluster) register(ctx context.Context) error {
	leaseSec := c.config.LeaseDuration.Seconds()

	var epoch int64
	var leaseExpiry, pgNow time.Time
	err := c.db.QueryRowContext(ctx, `
		INSERT INTO hosts (host_id, address, epoch, lease_expiry)
		VALUES ($1, $2, 1, now() + make_interval(secs => $3))
		ON CONFLICT (host_id) DO UPDATE
			SET epoch        = hosts.epoch + 1,
			    address      = EXCLUDED.address,
			    lease_expiry = now() + make_interval(secs => $3)
		RETURNING epoch, lease_expiry, now()
	`, c.config.HostID, c.config.Address, leaseSec).Scan(&epoch, &leaseExpiry, &pgNow)
	if err != nil {
		return err
	}

	now := time.Now()
	c.mu.Lock()
	c.epoch = epoch
	c.pgExpiry = leaseExpiry
	c.pgNow = pgNow
	c.localBase = now
	c.mu.Unlock()

	return nil
}

// --- lease renewal ---

func (c *Cluster) renewLoop() {
	defer c.wg.Done()
	interval := c.config.LeaseDuration / 3
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-c.done:
			return
		case <-ticker.C:
			if err := c.renewLease(context.Background()); err != nil {
				slog.Error("cluster lease renewal failed",
					"hostID", c.config.HostID, "error", err)
			}
		}
	}
}

func (c *Cluster) renewLease(ctx context.Context) error {
	leaseSec := c.config.LeaseDuration.Seconds()

	c.mu.RLock()
	epoch := c.epoch
	c.mu.RUnlock()

	var newExpiry, pgNow time.Time
	var returnedEpoch int64
	err := c.db.QueryRowContext(ctx, `
		UPDATE hosts
		SET lease_expiry = now() + make_interval(secs => $1)
		WHERE host_id = $2 AND epoch = $3
		RETURNING lease_expiry, epoch, now()
	`, leaseSec, c.config.HostID, epoch).Scan(&newExpiry, &returnedEpoch, &pgNow)
	if err != nil {
		atomic.AddInt64(&c.renewalFailures, 1)
		if err == sql.ErrNoRows {
			return fmt.Errorf("lease renewal: epoch mismatch (fenced), host %s epoch %d",
				c.config.HostID, epoch)
		}
		return err
	}

	// Reset failure count on success.
	atomic.StoreInt64(&c.renewalFailures, 0)

	now := time.Now()
	c.mu.Lock()
	c.pgExpiry = newExpiry
	c.pgNow = pgNow
	c.localBase = now
	c.mu.Unlock()

	return nil
}

// ConsecutiveRenewalFailures returns the number of consecutive lease
// renewal failures since the last success.
func (c *Cluster) ConsecutiveRenewalFailures() int64 {
	return atomic.LoadInt64(&c.renewalFailures)
}

// --- host polling + ring ---

func (c *Cluster) pollLoop() {
	defer c.wg.Done()
	ticker := time.NewTicker(c.config.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.done:
			return
		case <-ticker.C:
			if err := c.pollHosts(context.Background()); err != nil {
				slog.Error("cluster poll failed",
					"hostID", c.config.HostID, "error", err)
			}
		}
	}
}

func (c *Cluster) pollHosts(ctx context.Context) error {
	rows, err := c.db.QueryContext(ctx, `
		SELECT host_id, address, epoch, lease_expiry
		FROM hosts
		WHERE lease_expiry > now()
		ORDER BY host_id
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	var hosts []HostInfo
	var members []string
	for rows.Next() {
		var h HostInfo
		if err := rows.Scan(&h.HostID, &h.Address, &h.Epoch, &h.LeaseExpiry); err != nil {
			return err
		}
		hosts = append(hosts, h)
		members = append(members, h.HostID)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	// Check if membership changed before rebuilding the ring.
	oldMembers := c.ring.Members()
	changed := len(oldMembers) != len(members)
	if !changed {
		for i := range oldMembers {
			if oldMembers[i] != members[i] {
				changed = true
				break
			}
		}
	}

	c.mu.Lock()
	c.hosts = hosts
	c.mu.Unlock()
	c.storeHostsSnapshot(hosts)

	if changed {
		c.ring.Set(members)
		slog.Info("cluster ring rebuilt", "members", members)
	}

	return nil
}
