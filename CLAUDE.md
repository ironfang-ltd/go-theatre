# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Test Commands

```bash
go build ./...                          # Build all packages
go test ./...                           # Run all tests
go test -v ./...                        # Run tests with verbose output
go test -run TestRequest ./...          # Run a single test
go test -bench BenchmarkHost_Request ./...  # Run benchmarks
```

## Architecture

`go-theatre` is a lightweight actor model framework for Go. Single package (`theatre`), zero external dependencies, requires Go 1.24.2+.

### Core Components

**Host** — The runtime container. Manages actor lifecycles, routes messages via inbox/outbox channels, tracks request/response pairs, and runs periodic cleanup (idle actors removed after 15s, expired requests after 5s).

**Actor** — Each actor runs in its own goroutine, processing messages sequentially from an inbox channel. Actors are created lazily on first message and auto-shutdown after 15 seconds of inactivity.

**Receiver** — The primary extension interface. Implement `Receive(ctx *Context) error` to define actor behavior.

**Context** — Passed to `Receiver.Receive()`. Provides `Send()`, `Request()`, and `Reply()` methods for actor-to-actor and actor-to-host communication.

**Ref** (Type + ID) identifies an actor. **HostRef** (IP + Port + Epoch) identifies a host.

### Message Flow

```
Host.Send/Request → outbox channel → Host processes → finds/creates Actor → Actor inbox → Receiver.Receive(ctx) → ctx.Send/Request/Reply → outbox
```

Two messaging patterns: fire-and-forget (`Send`) and request/response (`Request` with timeout).

### Distributed Foundation

`Directory` interface and `HostRef` type exist for distributed actor lookups but are not yet integrated into message routing.

### Concurrency Model

- Channels for inter-actor messaging
- Mutexes in ActorRegistry, RequestManager, Directory
- Atomic operations for actor status and counters
- `sync.Pool` for Response and Request object reuse
- Panic recovery with stack traces in actor receive handlers
