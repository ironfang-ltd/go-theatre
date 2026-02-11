package theatre

import (
	"log/slog"
	"runtime"
	"time"
)

type DeadLetterHandler func(msg InboxMessage)

type Option func(*hostConfig)

type hostConfig struct {
	idleTimeout       time.Duration
	requestTimeout    time.Duration
	cleanupInterval   time.Duration
	drainTimeout      time.Duration
	placementTTL      time.Duration
	deadLetterHandler DeadLetterHandler

	// Freeze configuration (cluster mode).
	freezeGracePeriod  time.Duration // time to wait for actors to exit after ctx cancel
	safetyMargin       time.Duration // trigger freeze when remaining lease < this
	maxRenewalFailures int           // trigger freeze after N consecutive renewal failures

	// Throughput tuning.
	actorInboxSize int  // per-actor inbox buffer (default 64)
	hostInboxSize  int  // host inbox channel buffer (default 4096)
	inboxWorkers   int  // number of processInbox goroutines (default GOMAXPROCS)
	outboxWorkers  int  // number of processOutbox goroutines (default GOMAXPROCS)
	outboxSize     int  // host outbox channel buffer (default 4096)
	panicRecovery  bool // wrap receiver.Receive in defer/recover (default true)

	// Admin server address (e.g. "127.0.0.1:9090"). Empty = disabled.
	adminAddr string

	// Log level for the structured JSON logger. Default: slog.LevelInfo.
	logLevel slog.Level

	// Scheduler recovery interval for overdue DB schedules (cluster mode).
	scheduleRecoveryInterval time.Duration

	// Test hooks (nil in production).
	postClaimHook func(Ref) // called after successful ownership claim, before actor start
}

func defaultHostConfig() hostConfig {
	return hostConfig{
		idleTimeout:        15 * time.Second,
		requestTimeout:     5 * time.Second,
		cleanupInterval:    1 * time.Second,
		drainTimeout:       5 * time.Second,
		placementTTL:       10 * time.Second,
		freezeGracePeriod:  2 * time.Second,
		safetyMargin:       3 * time.Second,
		maxRenewalFailures: 2,
		actorInboxSize:     64,
		hostInboxSize:      4096,
		inboxWorkers:       runtime.GOMAXPROCS(0),
		outboxWorkers:      runtime.GOMAXPROCS(0),
		outboxSize:         4096,
		panicRecovery:              true,
		scheduleRecoveryInterval:   10 * time.Second,
	}
}

func WithIdleTimeout(d time.Duration) Option {
	return func(c *hostConfig) {
		c.idleTimeout = d
	}
}

func WithRequestTimeout(d time.Duration) Option {
	return func(c *hostConfig) {
		c.requestTimeout = d
	}
}

func WithCleanupInterval(d time.Duration) Option {
	return func(c *hostConfig) {
		c.cleanupInterval = d
	}
}

func WithDrainTimeout(d time.Duration) Option {
	return func(c *hostConfig) {
		c.drainTimeout = d
	}
}

func WithPlacementTTL(d time.Duration) Option {
	return func(c *hostConfig) {
		c.placementTTL = d
	}
}

func WithDeadLetterHandler(h DeadLetterHandler) Option {
	return func(c *hostConfig) {
		c.deadLetterHandler = h
	}
}

func WithFreezeGracePeriod(d time.Duration) Option {
	return func(c *hostConfig) {
		c.freezeGracePeriod = d
	}
}

func WithSafetyMargin(d time.Duration) Option {
	return func(c *hostConfig) {
		c.safetyMargin = d
	}
}

func WithMaxRenewalFailures(n int) Option {
	return func(c *hostConfig) {
		c.maxRenewalFailures = n
	}
}

func WithAdminAddr(addr string) Option {
	return func(c *hostConfig) {
		c.adminAddr = addr
	}
}

func WithLogLevel(level slog.Level) Option {
	return func(c *hostConfig) {
		c.logLevel = level
	}
}

// WithActorInboxSize sets the buffer size for each actor's inbox channel.
// A larger buffer allows processInbox to deliver messages without blocking
// when the actor is busy. Default: 64.
func WithActorInboxSize(n int) Option {
	return func(c *hostConfig) {
		c.actorInboxSize = n
	}
}

// WithHostInboxSize sets the buffer size for the host's central inbox channel.
// Default: 4096.
func WithHostInboxSize(n int) Option {
	return func(c *hostConfig) {
		c.hostInboxSize = n
	}
}

// WithInboxWorkers sets the number of goroutines consuming the host inbox.
// More workers allow parallel message dispatch at the cost of relaxed
// cross-actor message ordering (per-actor ordering is always preserved).
// Default: runtime.GOMAXPROCS(0).
func WithInboxWorkers(n int) Option {
	return func(c *hostConfig) {
		c.inboxWorkers = n
	}
}

// WithOutboxWorkers sets the number of goroutines consuming the host outbox.
// More workers allow parallel message routing in cluster mode. All routing
// functions are goroutine-safe (sharded locks, sync.Map, per-peer mutex).
// Default: runtime.GOMAXPROCS(0).
func WithOutboxWorkers(n int) Option {
	return func(c *hostConfig) {
		c.outboxWorkers = n
	}
}

// WithOutboxSize sets the buffer size for the host's outbox channel.
// Default: 4096.
func WithOutboxSize(n int) Option {
	return func(c *hostConfig) {
		c.outboxSize = n
	}
}

// WithPanicRecovery controls whether actor Receive calls are wrapped in a
// defer/recover block. Enabled by default. Disabling eliminates the
// per-message defer+closure overhead (~50-100ns) but means a panic in a
// Receiver will crash the entire process instead of being caught.
func WithPanicRecovery(enabled bool) Option {
	return func(c *hostConfig) {
		c.panicRecovery = enabled
	}
}

// WithScheduleRecoveryInterval sets how often the scheduler polls the database
// for overdue schedules (cluster mode only). Default: 10s.
func WithScheduleRecoveryInterval(d time.Duration) Option {
	return func(c *hostConfig) {
		c.scheduleRecoveryInterval = d
	}
}

// WithPostClaimHook installs a function called after a successful ownership
// claim but before the actor goroutine starts. Test-only; used in chaos tests
// to simulate crashes between claim and activation.
func WithPostClaimHook(fn func(Ref)) Option {
	return func(c *hostConfig) {
		c.postClaimHook = fn
	}
}
