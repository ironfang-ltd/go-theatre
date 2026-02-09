package main

import (
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ironfang-ltd/go-theatre"
)

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

func main() {
	duration := flag.Duration("duration", 30*time.Second, "test duration")
	workers := flag.Int("workers", 10, "worker goroutines per host")
	actors := flag.Int("actors", 500, "actor ID pool size")
	flag.Parse()

	fmt.Printf("go-theatre load test\n")
	fmt.Printf("  duration: %s\n", *duration)
	fmt.Printf("  workers:  %d per host (x3 = %d total)\n", *workers, *workers*3)
	fmt.Printf("  actors:   %d IDs in pool\n\n", *actors)

	var inits, shutdowns atomic.Int64

	hosts := make([]*hostEntry, 3)
	for i := range 3 {
		h := theatre.NewHost(
			theatre.WithIdleTimeout(2*time.Second),
			theatre.WithRequestTimeout(3*time.Second),
			theatre.WithCleanupInterval(500*time.Millisecond),
			theatre.WithAdminAddr("127.0.0.1:"+strconv.Itoa(8081+i)),
		)
		h.RegisterActor("worker", func() theatre.Receiver {
			return &loadReceiver{inits: &inits, shutdowns: &shutdowns}
		})
		hosts[i] = &hostEntry{host: h, name: fmt.Sprintf("host-%d", i+1)}
	}

	for _, he := range hosts {
		he.host.Start()
	}

	fmt.Printf("hosts started (admin ports 8081-8083)\n\n")

	// Shared stop signal for all workers.
	stop := make(chan struct{})
	start := time.Now()

	var wg sync.WaitGroup
	var totalSends, totalRequests, totalReplyErrors atomic.Int64

	for _, he := range hosts {
		for range *workers {
			wg.Add(1)
			go func(h *theatre.Host) {
				defer wg.Done()
				for {
					select {
					case <-stop:
						return
					default:
					}

					id := strconv.Itoa(rand.IntN(*actors))
					ref := theatre.NewRef("worker", id)

					if rand.Float64() < 0.7 {
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
			}(he.host)
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

	os.Exit(0)
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
