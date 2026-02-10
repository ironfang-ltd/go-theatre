package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ironfang-ltd/go-theatre"
	_ "github.com/jackc/pgx/v5/stdlib"
)

type profile struct {
	name        string
	actors      int
	workers     int
	actorInbox  int
	hostInbox   int
	idleTimeout time.Duration
	memLimitGiB int64
}

var profiles = map[string]profile{
	"small": {
		name:        "small",
		actors:      1_000,
		workers:     10,
		actorInbox:  64,
		hostInbox:   4096,
		idleTimeout: 2 * time.Second,
		memLimitGiB: 2,
	},
	"medium": {
		name:        "medium",
		actors:      10_000,
		workers:     20,
		actorInbox:  64,
		hostInbox:   8192,
		idleTimeout: 5 * time.Second,
		memLimitGiB: 2,
	},
	"large": {
		name:        "large",
		actors:      100_000,
		workers:     50,
		actorInbox:  32,
		hostInbox:   16384,
		idleTimeout: 10 * time.Second,
		memLimitGiB: 4,
	},
	"massive": {
		name:        "massive",
		actors:      1_000_000,
		workers:     100,
		actorInbox:  8,
		hostInbox:   32768,
		idleTimeout: 30 * time.Second,
		memLimitGiB: 8,
	},
}

// loadReceiver is a simple actor that echoes string messages and tracks lifecycle events.
type loadReceiver struct {
	inits     *atomic.Int64
	shutdowns *atomic.Int64
}

func (r *loadReceiver) Receive(ctx *theatre.Context) error {
	switch msg := ctx.Message.(type) {
	case theatre.Initialize:
		r.inits.Add(1)
	case theatre.Shutdown:
		r.shutdowns.Add(1)
	case string:
		ctx.Reply(msg)
	}
	return nil
}

type hostEntry struct {
	host *theatre.Host
	name string
}

func hostOptions(p profile, index int) []theatre.Option {
	return []theatre.Option{
		theatre.WithIdleTimeout(p.idleTimeout),
		theatre.WithRequestTimeout(3 * time.Second),
		theatre.WithCleanupInterval(500 * time.Millisecond),
		theatre.WithPanicRecovery(false),
		theatre.WithActorInboxSize(p.actorInbox),
		theatre.WithHostInboxSize(p.hostInbox),
		theatre.WithOutboxSize(p.hostInbox),
		theatre.WithOutboxWorkers(p.workers),
		theatre.WithAdminAddr("127.0.0.1:" + strconv.Itoa(8081+index)),
	}
}

func main() {
	profileName := flag.String("profile", "small", "preset profile: small, medium, large, massive")
	hostCount := flag.Int("hosts", 3, "number of hosts (1=standalone)")
	actorsFlag := flag.Int("actors", 0, "actor pool size (overrides profile)")
	workersFlag := flag.Int("workers", 0, "workers per host (overrides profile)")
	duration := flag.Duration("duration", 30*time.Second, "test duration")
	memlimit := flag.Int64("memlimit", -1, "GOMEMLIMIT in GiB (0=disabled, -1=from profile)")
	sendpct := flag.Int("sendpct", 70, "percentage of Send vs Request (0-100)")
	mode := flag.String("mode", "mixed", "worker mode: mixed (random actors via all hosts), forward (all workers enter host-1), local (workers only target local actors)")
	dsn := flag.String("dsn", "", "Postgres connection string (empty = in-memory ring mode)")
	cpuprofile := flag.String("cpuprofile", "", "write CPU profile to file")
	memprofile := flag.String("memprofile", "", "write allocation profile to file")
	flag.Parse()

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "could not create CPU profile: %v\n", err)
			os.Exit(1)
		}
		pprof.StartCPUProfile(f)
		defer func() {
			pprof.StopCPUProfile()
			f.Close()
		}()
	}

	if *memprofile != "" {
		defer func() {
			f, err := os.Create(*memprofile)
			if err != nil {
				fmt.Fprintf(os.Stderr, "could not create memory profile: %v\n", err)
				return
			}
			runtime.GC()
			pprof.WriteHeapProfile(f)
			f.Close()
		}()
	}

	p, ok := profiles[*profileName]
	if !ok {
		fmt.Fprintf(os.Stderr, "unknown profile %q (valid: small, medium, large, massive)\n", *profileName)
		os.Exit(1)
	}

	// Apply overrides.
	if *actorsFlag > 0 {
		p.actors = *actorsFlag
	}
	if *workersFlag > 0 {
		p.workers = *workersFlag
	}
	if *memlimit >= 0 {
		p.memLimitGiB = *memlimit
	}
	if *sendpct < 0 || *sendpct > 100 {
		fmt.Fprintf(os.Stderr, "sendpct must be 0-100\n")
		os.Exit(1)
	}
	switch *mode {
	case "mixed", "forward", "local":
	default:
		fmt.Fprintf(os.Stderr, "unknown mode %q (valid: mixed, forward, local)\n", *mode)
		os.Exit(1)
	}
	if *mode != "mixed" && *hostCount <= 1 {
		fmt.Fprintf(os.Stderr, "mode %q requires --hosts > 1\n", *mode)
		os.Exit(1)
	}

	totalWorkers := p.workers * *hostCount

	// GC tuning.
	gcInfo := "GOGC=default"
	if p.memLimitGiB > 0 {
		debug.SetMemoryLimit(p.memLimitGiB * 1024 * 1024 * 1024)
		debug.SetGCPercent(-1)
		gcInfo = fmt.Sprintf("GOGC=off  GOMEMLIMIT=%dGiB", p.memLimitGiB)
	}

	// Determine mode label for banner.
	modeLabel := "standalone"
	if *hostCount > 1 {
		if *dsn != "" {
			modeLabel = "postgres"
		} else {
			modeLabel = "ring-only"
		}
	}

	// Startup banner.
	fmt.Printf("go-theatre load test\n")
	fmt.Printf("  profile:  %s\n", p.name)
	fmt.Printf("  hosts:    %d (%s)\n", *hostCount, modeLabel)
	fmt.Printf("  actors:   %d\n", p.actors)
	fmt.Printf("  workers:  %d per host (x%d = %d total)\n", p.workers, *hostCount, totalWorkers)
	fmt.Printf("  mode:     %s\n", *mode)
	fmt.Printf("  mix:      %d%% send / %d%% request\n", *sendpct, 100-*sendpct)
	fmt.Printf("  duration: %s\n", *duration)
	fmt.Printf("  GC:       %s\n", gcInfo)
	fmt.Printf("  inbox:    actor=%d  host=%d\n", p.actorInbox, p.hostInbox)
	fmt.Println()

	var inits, shutdowns atomic.Int64

	var hosts []*hostEntry
	var extraCleanup func()

	if *hostCount == 1 {
		// Standalone mode — no transport, no cluster.
		hosts = setupStandalone(p, &inits, &shutdowns)
	} else if *dsn != "" {
		// Postgres cluster mode.
		hosts, extraCleanup = setupPostgresCluster(p, *hostCount, *dsn, &inits, &shutdowns)
	} else {
		// Ring-only mode — transport + hash ring, no DB.
		hosts, extraCleanup = setupRingCluster(p, *hostCount, &inits, &shutdowns)
	}

	for _, he := range hosts {
		he.host.Start()
	}

	lastPort := 8080 + len(hosts)
	fmt.Printf("hosts started (admin ports 8081-%d)\n\n", lastPort)

	// Shared stop signal for all workers.
	stop := make(chan struct{})
	start := time.Now()

	var wg sync.WaitGroup
	var totalSends, totalRequests, totalReplyErrors atomic.Int64

	sendThreshold := float64(*sendpct) / 100.0

	// Pre-compute actor ID strings to avoid strconv.Itoa allocs in hot loop.
	actorIDs := make([]string, p.actors)
	for i := range actorIDs {
		actorIDs[i] = strconv.Itoa(i)
	}

	// Build per-host actor ID lists for "local" mode.
	// In local mode each worker only targets actors owned by its host,
	// eliminating cross-host forwarding.
	var perHostActorIDs [][]string
	if *mode == "local" && *hostCount > 1 {
		ring := theatre.NewHashRing()
		hostIDs := make([]string, *hostCount)
		for i := range hostIDs {
			hostIDs[i] = fmt.Sprintf("host-%d", i+1)
		}
		ring.Set(hostIDs)

		perHostActorIDs = make([][]string, *hostCount)
		for _, id := range actorIDs {
			owner, _ := ring.Lookup("worker:" + id)
			for hi, hid := range hostIDs {
				if hid == owner {
					perHostActorIDs[hi] = append(perHostActorIDs[hi], id)
					break
				}
			}
		}
		for hi, ids := range perHostActorIDs {
			fmt.Printf("  %s owns %d actors\n", hosts[hi].name, len(ids))
		}
		fmt.Println()
	}

	// spawnWorkers launches n goroutines that send to h,
	// picking from the given actor ID pool.
	spawnWorkers := func(h *theatre.Host, pool []string, n int) {
		poolSize := len(pool)
		for range n {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-stop:
						return
					default:
					}

					ref := theatre.NewRef("worker", pool[rand.IntN(poolSize)])

					if rand.Float64() < sendThreshold {
						if err := h.Send(ref, "ping"); err != nil {
							if err == theatre.ErrHostDraining {
								return
							}
							continue
						}
						totalSends.Add(1)
					} else {
						_, err := h.Request(ref, "echo")
						if err != nil {
							totalReplyErrors.Add(1)
						}
						totalRequests.Add(1)
					}
				}
			}()
		}
	}

	switch *mode {
	case "mixed":
		// Default: workers on all hosts, random actors. Tests cross-host forwarding.
		for _, he := range hosts {
			spawnWorkers(he.host, actorIDs, p.workers)
		}
	case "forward":
		// All workers enter through host-1. Maximum forwarding pressure.
		spawnWorkers(hosts[0].host, actorIDs, totalWorkers)
	case "local":
		// Each host's workers only target actors owned by that host. No forwarding.
		for hi, he := range hosts {
			if len(perHostActorIDs[hi]) == 0 {
				continue
			}
			spawnWorkers(he.host, perHostActorIDs[hi], p.workers)
		}
	}

	// Progress reporting.
	ticker := time.NewTicker(5 * time.Second)
	go func() {
		for range ticker.C {
			elapsed := time.Since(start).Truncate(time.Second)
			printProgress(hosts, elapsed, &inits, &shutdowns)
		}
	}()

	// Wait for duration, then signal all workers to stop.
	time.Sleep(*duration)
	close(stop)
	wg.Wait()
	ticker.Stop()

	fmt.Printf("\n--- stopping hosts ---\n")
	var stopWg sync.WaitGroup
	for _, he := range hosts {
		stopWg.Add(1)
		go func(h *theatre.Host) {
			defer stopWg.Done()
			h.Stop()
		}(he.host)
	}
	stopWg.Wait()

	if extraCleanup != nil {
		extraCleanup()
	}

	// Final summary.
	elapsed := time.Since(start)
	fmt.Printf("\n=== FINAL SUMMARY ===\n")
	fmt.Printf("  Duration:        %s\n", elapsed.Truncate(time.Millisecond))
	fmt.Printf("  Total sends:     %d\n", totalSends.Load())
	fmt.Printf("  Total requests:  %d\n", totalRequests.Load())
	fmt.Printf("  Reply errors:    %d\n", totalReplyErrors.Load())
	fmt.Printf("  Actor inits:     %d\n", inits.Load())
	fmt.Printf("  Actor shutdowns: %d\n", shutdowns.Load())
	totalOps := totalSends.Load() + totalRequests.Load()
	fmt.Printf("  Aggregate RPS:   %.0f\n\n", float64(totalOps)/elapsed.Seconds())

	printProgress(hosts, elapsed.Truncate(time.Second), &inits, &shutdowns)
}

// setupStandalone creates a single standalone host (no transport, no cluster).
func setupStandalone(p profile, inits, shutdowns *atomic.Int64) []*hostEntry {
	h := theatre.NewHost(hostOptions(p, 0)...)
	h.RegisterActor("worker", func() theatre.Receiver {
		return &loadReceiver{inits: inits, shutdowns: shutdowns}
	})
	return []*hostEntry{{host: h, name: "host-1"}}
}

// setupRingCluster creates N hosts with TCP transport and a shared hash ring
// for deterministic actor placement. No Postgres database is used.
func setupRingCluster(p profile, n int, inits, shutdowns *atomic.Int64) ([]*hostEntry, func()) {
	hosts := make([]*hostEntry, n)
	transports := make([]*theatre.Transport, n)
	hostIDs := make([]string, n)

	// Create hosts and transports.
	for i := range n {
		hostID := fmt.Sprintf("host-%d", i+1)
		hostIDs[i] = hostID

		h := theatre.NewHost(hostOptions(p, i)...)
		h.RegisterActor("worker", func() theatre.Receiver {
			return &loadReceiver{inits: inits, shutdowns: shutdowns}
		})
		hosts[i] = &hostEntry{host: h, name: hostID}

		t, err := theatre.NewTransport(hostID, "127.0.0.1:0", h.HandleTransportMessage)
		if err != nil {
			fmt.Fprintf(os.Stderr, "transport error: %v\n", err)
			os.Exit(1)
		}
		t.Start()
		transports[i] = t
	}

	// Build shared host info and ring members.
	hostInfos := make([]theatre.HostInfo, n)
	for i := range n {
		hostInfos[i] = theatre.HostInfo{
			HostID:  hostIDs[i],
			Address: transports[i].Addr(),
			Epoch:   1,
		}
	}

	// Wire each host with a ring-only cluster and transport.
	for i := range n {
		c := theatre.NewRingOnlyCluster(hostIDs[i], transports[i].Addr(), 1)
		c.SetHosts(hostInfos)
		c.Ring().Set(hostIDs)

		hosts[i].host.SetTransport(transports[i])
		hosts[i].host.SetCluster(c)
	}

	cleanup := func() {
		for _, t := range transports {
			t.Stop()
		}
	}
	return hosts, cleanup
}

// setupPostgresCluster creates N hosts backed by a real Postgres cluster
// with lease management, ownership claims, and transport.
func setupPostgresCluster(p profile, n int, dsn string, inits, shutdowns *atomic.Int64) ([]*hostEntry, func()) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "database open error: %v\n", err)
		os.Exit(1)
	}

	ctx := context.Background()
	if err := theatre.MigrateSchema(ctx, db); err != nil {
		fmt.Fprintf(os.Stderr, "schema migration error: %v\n", err)
		os.Exit(1)
	}

	hosts := make([]*hostEntry, n)
	transports := make([]*theatre.Transport, n)
	clusters := make([]*theatre.Cluster, n)

	for i := range n {
		hostID := fmt.Sprintf("host-%d", i+1)

		h := theatre.NewHost(hostOptions(p, i)...)
		h.RegisterActor("worker", func() theatre.Receiver {
			return &loadReceiver{inits: inits, shutdowns: shutdowns}
		})
		hosts[i] = &hostEntry{host: h, name: hostID}

		t, err := theatre.NewTransport(hostID, "127.0.0.1:0", h.HandleTransportMessage)
		if err != nil {
			fmt.Fprintf(os.Stderr, "transport error: %v\n", err)
			os.Exit(1)
		}
		t.Start()
		transports[i] = t

		c := theatre.NewCluster(db, theatre.ClusterConfig{
			HostID:  hostID,
			Address: t.Addr(),
		})
		if err := c.Start(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "cluster start error for %s: %v\n", hostID, err)
			os.Exit(1)
		}
		clusters[i] = c

		h.SetTransport(t)
		h.SetCluster(c)
	}

	cleanup := func() {
		for _, c := range clusters {
			c.Stop()
		}
		for _, t := range transports {
			t.Stop()
		}
		db.Close()
	}
	return hosts, cleanup
}

func printProgress(hosts []*hostEntry, elapsed time.Duration, inits, shutdowns *atomic.Int64) {
	secs := elapsed.Seconds()
	fmt.Printf("[%s] inits=%d shutdowns=%d\n", elapsed, inits.Load(), shutdowns.Load())
	fmt.Printf("  %-8s %10s %10s %10s %10s %10s %10s %8s %10s\n",
		"HOST", "SENT", "RECV", "DEAD", "REQ", "TIMEOUT", "ACTV_TOT", "ACTORS", "RPS")
	for _, he := range hosts {
		s := he.host.Metrics().Snapshot()
		ops := s["messages_sent"] + s["requests_total"]
		rps := float64(0)
		if secs > 0 {
			rps = float64(ops) / secs
		}
		fmt.Printf("  %-8s %10d %10d %10d %10d %10d %10d %8d %10.0f\n",
			he.name,
			s["messages_sent"],
			s["messages_received"],
			s["messages_dead_lettered"],
			s["requests_total"],
			s["requests_timed_out"],
			s["activations_total"],
			s["actors_active"],
			rps,
		)
	}
	fmt.Println()
}
