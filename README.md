# go-theatre

A lightweight actor model framework for Go. Single package, zero external runtime dependencies, requires Go 1.25.5+.

## Quick Start

```go
package main

import (
    "fmt"
    "github.com/ironfang-ltd/go-theatre"
)

type Greeter struct{}

func (g *Greeter) Receive(ctx *theatre.Context) error {
    switch msg := ctx.Message.(type) {
    case theatre.Initialize:
        fmt.Println("actor started")
    case string:
        fmt.Println("received:", msg)
        ctx.Reply("hello back!")
    }
    return nil
}

func main() {
    h := theatre.NewHost()
    h.RegisterActor("greeter", func() theatre.Receiver { return &Greeter{} })
    h.Start()
    defer h.Stop()

    ref := theatre.NewRef("greeter", "1")

    // Fire-and-forget
    h.Send(ref, "hi")

    // Request/reply with timeout
    resp, err := h.Request(ref, "hello")
    if err == nil {
        fmt.Println("reply:", resp)
    }
}
```

## Architecture

```
Host.Send/Request
  → outbox channel
    → Host routes to Actor (created lazily on first message)
      → Initialize message
        → Actor inbox (own goroutine)
          → Receiver.Receive(ctx)
            → ctx.Send / ctx.Request / ctx.Reply
              → outbox
```

### Core Concepts

| Concept | Description |
|---------|-------------|
| **Host** | Runtime container. Manages actor lifecycles, routes messages, tracks requests, runs periodic cleanup. |
| **Actor** | Runs in its own goroutine, processes messages sequentially from an inbox channel. Created lazily, auto-removed after idle timeout. |
| **Receiver** | The interface you implement: `Receive(ctx *Context) error`. |
| **Context** | Passed to `Receive()`. Provides `Send()`, `Request()`, `Reply()` and exposes `ActorRef`, `Message`, and a cancellation `Ctx`. |
| **Ref** | Actor identity as `Type:ID` (e.g. `"player:42"`). |

### Messaging Patterns

- **Send** — fire-and-forget, non-blocking
- **Request** — caller blocks until reply or timeout (`WithRequestTimeout`, default 5s)

### Lifecycle Messages

Every actor receives `Initialize` before any user messages, and `Shutdown` when being stopped. Return `ErrStopActor` from `Receive` to self-stop.

## Configuration

```go
h := theatre.NewHost(
    theatre.WithIdleTimeout(15 * time.Second),       // remove inactive actors
    theatre.WithRequestTimeout(5 * time.Second),     // request expiry
    theatre.WithCleanupInterval(1 * time.Second),    // cleanup tick rate
    theatre.WithDrainTimeout(5 * time.Second),       // graceful shutdown timeout
    theatre.WithActorInboxSize(64),                  // per-actor channel buffer
    theatre.WithHostInboxSize(4096),                 // central inbox buffer
    theatre.WithInboxWorkers(runtime.GOMAXPROCS(0)), // parallel dispatch goroutines
    theatre.WithPanicRecovery(true),                 // defer/recover in receive loop
    theatre.WithAdminAddr("127.0.0.1:9090"),         // enable admin HTTP server
    theatre.WithLogLevel(slog.LevelInfo),            // structured logging level
    theatre.WithDeadLetterHandler(func(msg theatre.InboxMessage) {
        log.Printf("dead letter: %v", msg)
    }),
)
```

## Admin Server

When `WithAdminAddr` is configured, the host exposes HTTP endpoints:

| Endpoint | Description |
|----------|-------------|
| `GET /cluster/status` | Host state, metrics, registered actor types |
| `GET /cluster/hosts` | Live cluster members |
| `GET /cluster/actor?type=T&id=ID` | Actor ownership info |
| `GET /cluster/local-actor?type=T&id=ID` | Local actor state |
| `GET /debug/vars` | expvar metrics (messages sent/received, requests, actors active, etc.) |

## Cluster Mode

go-theatre supports two distributed modes for running actors across multiple hosts. Both use TCP transport and a consistent hash ring for deterministic actor placement.

| Mode | Transport | Placement | Ownership | Lease / Failover | Use Case |
|------|-----------|-----------|-----------|------------------|----------|
| **Ring-only** | TCP | Hash ring | Implicit (ring decides) | None | Development, testing, benchmarks |
| **Postgres** | TCP | Hash ring + DB claims | Postgres `actor_ownership` table | Lease-based with freeze/failover | Production |

### Ring-Only Mode

No database required. Each host gets a `RingOnlyCluster` with a shared hash ring. All hosts agree on actor placement deterministically. There are no ownership claims, no leases, and no failover — if a host dies, its actors are lost.

```go
// Create hosts with transport.
h := theatre.NewHost()
t, _ := theatre.NewTransport("host-1", "127.0.0.1:0", h.HandleTransportMessage)
t.Start()

// Create a ring-only cluster (no DB, no background goroutines).
c := theatre.NewRingOnlyCluster("host-1", t.Addr(), 1)
c.SetHosts([]theatre.HostInfo{
    {HostID: "host-1", Address: t.Addr(), Epoch: 1},
    {HostID: "host-2", Address: otherAddr, Epoch: 1},
})
c.Ring().Set([]string{"host-1", "host-2"})

h.SetTransport(t)
h.SetCluster(c)
h.Start()
```

### Postgres Mode

Full distributed mode with Postgres-backed ownership, lease management, advisory locks, and automatic failover via the freeze protocol.

```go
db, _ := sql.Open("pgx", dsn)
theatre.MigrateSchema(ctx, db)

h := theatre.NewHost()
t, _ := theatre.NewTransport("host-1", "127.0.0.1:0", h.HandleTransportMessage)
t.Start()

c := theatre.NewCluster(db, theatre.ClusterConfig{
    HostID:  "host-1",
    Address: t.Addr(),
})
c.Start(ctx)

h.SetTransport(t)
h.SetCluster(c)
h.Start()
```

### Integration Tests

Set `THEATRE_TEST_DSN` to run Postgres-backed integration and cluster tests:

```bash
export THEATRE_TEST_DSN="postgres://theatre:theatre@localhost:5432/theatre_test?sslmode=disable"
```

Tests that require Postgres check for this variable and skip if it's not set.

## Commands

### Playground

Interactive demo that spins up 3 standalone hosts with echo actors, sends messages and requests, then blocks so you can explore the admin endpoints.

```bash
go run ./cmd/playground
```

Once running, try:

```bash
curl http://127.0.0.1:9090/cluster/status
curl http://127.0.0.1:9090/cluster/local-actor?type=echo&id=1
curl http://127.0.0.1:9090/debug/vars
```

Admin ports: host-1 on `9090`, host-2 on `9091`, host-3 on `9092`. Press `Ctrl+C` to stop.

### Load Test

Configurable load test with preset profiles for benchmarking at different scales. Supports three cluster topologies and three worker distribution modes.

**Cluster topologies** (controlled by `-hosts` and `-dsn`):

- **`-hosts 1`** — Standalone. Single host, no networking.
- **`-hosts N`** (no `-dsn`) — Ring-only. N hosts with TCP transport and in-memory hash ring. No database.
- **`-hosts N -dsn "..."`** — Postgres. N hosts with TCP transport and full Postgres cluster.

**Worker modes** (controlled by `-mode`):

- **`mixed`** (default) — Workers spread across all hosts, targeting random actors. Tests realistic cross-host forwarding via the hash ring.
- **`forward`** — All workers enter through host-1. Maximizes forwarding pressure on a single entry point.
- **`local`** — Each host's workers only target actors owned by that host (pre-computed via the hash ring). Zero cross-host traffic — tests pure local throughput.

```bash
# Standalone (single host, no networking)
go run ./cmd/loadtest -profile small -hosts 1 -duration 10s

# Multi-host ring-only with default mixed mode
go run ./cmd/loadtest -profile medium -hosts 3 -duration 30s

# Forward mode — all workers funnel through host-1
go run ./cmd/loadtest -profile medium -hosts 3 -mode forward -duration 30s

# Local mode — no cross-host forwarding
go run ./cmd/loadtest -profile medium -hosts 3 -mode local -duration 30s

# Multi-host with Postgres cluster
go run ./cmd/loadtest -profile medium -hosts 3 -dsn "postgres://user:pass@localhost:5432/theatre?sslmode=disable" -duration 30s

# Custom overrides
go run ./cmd/loadtest -hosts 1 -actors 5000 -workers 25 -duration 20s

# CPU / memory profiling
go run ./cmd/loadtest -profile medium -hosts 3 -duration 15s -cpuprofile cpu.prof -memprofile mem.prof
go tool pprof -http :8080 cpu.prof
go tool pprof -top -alloc_space mem.prof
```

#### Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-profile` | `small` | Preset: `small`, `medium`, `large`, `massive` |
| `-hosts` | `3` | Number of hosts (1 = standalone) |
| `-mode` | `mixed` | Worker mode: `mixed`, `forward`, `local` |
| `-dsn` | _(empty)_ | Postgres connection string (empty = ring-only mode) |
| `-actors` | from profile | Actor pool size (overrides profile) |
| `-workers` | from profile | Workers per host (overrides profile) |
| `-duration` | `30s` | Test duration |
| `-memlimit` | from profile | GOMEMLIMIT in GiB (0 = disabled) |
| `-sendpct` | `70` | Percentage Send vs Request (0-100) |
| `-cpuprofile` | _(empty)_ | Write CPU profile to file |
| `-memprofile` | _(empty)_ | Write allocation profile to file |

#### Profiles

| Profile | Actors | Workers/host | Actor Inbox | Host Inbox | Idle Timeout | MemLimit |
|---------|--------|-------------|-------------|------------|-------------|----------|
| `small` | 1,000 | 10 | 64 | 4,096 | 2s | 2 GiB |
| `medium` | 10,000 | 20 | 64 | 8,192 | 5s | 2 GiB |
| `large` | 100,000 | 50 | 32 | 16,384 | 10s | 4 GiB |
| `massive` | 1,000,000 | 100 | 8 | 32,768 | 30s | 8 GiB |

- **small** — Quick smoke test. 1K actors fit comfortably in memory with default inbox sizes. Good for validating changes before pushing.
- **medium** — Realistic workload. 10K actors with moderate concurrency. Useful for profiling hot paths and measuring baseline throughput.
- **large** — Stress test. 100K actors with reduced actor inbox (32) to keep memory under 4 GiB across 3 hosts. Exposes contention in the registry and request manager.
- **massive** — Scale limit test. 1M actors with minimal inbox (8) to fit in 8 GiB. The longer idle timeout (30s) keeps actors alive for the full run, testing peak memory and GC behavior under pressure.

Admin ports start at `8081` and increment per host.

## Build & Test

```bash
go build ./...                                       # build all
go test ./...                                        # run unit tests
go test -v ./...                                     # verbose
go test -run TestRequest ./...                       # single test
go test -bench BenchmarkHost_Request ./...           # benchmarks

# Integration tests (requires Postgres)
THEATRE_TEST_DSN="postgres://theatre:theatre@localhost:5432/theatre_test?sslmode=disable" \
  go test -v ./...
```
