// playground-load generates traffic against a running playground cluster
// so the dashboard shows live stats and actor activity.
//
// Usage:
//
//	go run ./cmd/playground          # start playground first
//	go run ./cmd/playground-load     # start load generator
//
// Flags:
//
//	-workers   concurrent sender goroutines (default 60)
//	-actors    number of distinct actor IDs to target (default 1000)
//	-req-pct   percentage of messages that are requests vs sends (default 30)
//	-admin     admin address of any playground host (default 127.0.0.1:9090)
//	-type      actor type to target (default "bench" — nop receiver, no stdout)
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"runtime/debug"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ironfang-ltd/go-theatre"
)

type nopReceiver struct{}

func (nopReceiver) Receive(*theatre.Context) error { return nil }

func main() {
	workers := flag.Int("workers", 60, "number of concurrent sender goroutines")
	actors := flag.Int("actors", 1000, "number of distinct actor IDs to target")
	reqPct := flag.Int("req-pct", 30, "percentage of messages that are request/response (vs fire-and-forget)")
	admin := flag.String("admin", "127.0.0.1:9090", "admin address of any playground host")
	actorType := flag.String("type", "bench", "actor type to target (bench=nop, echo=prints)")
	memlimit := flag.Int64("memlimit", 2, "GOMEMLIMIT in GiB (0=disabled)")
	flag.Parse()

	// GC tuning: disable percentage-based GC, only collect near memory limit.
	if *memlimit > 0 {
		debug.SetMemoryLimit(*memlimit * 1024 * 1024 * 1024)
		debug.SetGCPercent(-1)
	}

	// --- Discover playground hosts ---

	resp, err := http.Get("http://" + *admin + "/cluster/hosts")
	if err != nil {
		log.Fatalf("cannot reach playground at %s: %v", *admin, err)
	}
	defer resp.Body.Close()

	var hostsResp struct {
		Hosts []struct {
			HostID    string `json:"host_id"`
			Address   string `json:"address"`
			AdminAddr string `json:"admin_addr"`
			Epoch     int64  `json:"epoch"`
		} `json:"hosts"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&hostsResp); err != nil {
		log.Fatalf("decode hosts: %v", err)
	}
	if len(hostsResp.Hosts) == 0 {
		log.Fatal("no hosts found in playground cluster")
	}

	fmt.Printf("Discovered %d playground hosts:\n", len(hostsResp.Hosts))
	for _, h := range hostsResp.Hosts {
		fmt.Printf("  %s  transport=%s  admin=%s\n", h.HostID, h.Address, h.AdminAddr)
	}
	fmt.Println()

	// --- Set up load generator host ---

	const hostID = "load-gen"

	// Match the playground's transport tuning so multi-conn works (both
	// sides must have it enabled).
	lanes := max(*workers/5, 1)
	if lanes > 8 {
		lanes = 8
	}

	h := theatre.NewHost(
		theatre.WithIdleTimeout(5*time.Minute),
		theatre.WithPanicRecovery(false),
		theatre.WithHostInboxSize(16384),
		theatre.WithOutboxSize(16384),
		theatre.WithOutboxWorkers(*workers),
		theatre.WithRequestTimeout(3*time.Second),
		theatre.WithCleanupInterval(500*time.Millisecond),
	)

	// Register actor types so Send/Request passes the descriptor check.
	h.RegisterActor("echo", func() theatre.Receiver { return nopReceiver{} })
	h.RegisterActor("ticker", func() theatre.Receiver { return nopReceiver{} })
	h.RegisterActor("bench", func() theatre.Receiver { return nopReceiver{} })

	t, err := theatre.NewTransport(hostID, "127.0.0.1:0", h.HandleTransportMessage)
	if err != nil {
		log.Fatalf("transport: %v", err)
	}
	t.SetSendLanes(lanes)
	t.SetMultiConn(lanes)
	t.Start()

	// Use the playground's ring members (NOT including load-gen) so
	// our ring hashes match the playground's exactly.
	ringMembers := make([]string, 0, len(hostsResp.Hosts))
	hostInfos := make([]theatre.HostInfo, 0, len(hostsResp.Hosts))
	for _, ph := range hostsResp.Hosts {
		ringMembers = append(ringMembers, ph.HostID)
		hostInfos = append(hostInfos, theatre.HostInfo{
			HostID:    ph.HostID,
			Address:   ph.Address,
			AdminAddr: ph.AdminAddr,
			Epoch:     ph.Epoch,
		})
	}

	cluster := theatre.NewRingOnlyCluster(hostID, t.Addr(), 1)
	cluster.Ring().Set(ringMembers)
	cluster.SetHosts(hostInfos)

	h.SetTransport(t)
	h.SetCluster(cluster)
	h.Start()

	// Pre-compute actor IDs to avoid fmt.Sprintf allocation per message.
	actorIDs := make([]string, *actors)
	for i := range actorIDs {
		actorIDs[i] = "load-" + strconv.Itoa(i)
	}
	poolSize := len(actorIDs)
	sendThreshold := float64(100-*reqPct) / 100.0

	fmt.Printf("Load generator started (transport=%s)\n", t.Addr())
	fmt.Printf("Workers: %d  Actors: %d  Type: %s  Request%%: %d%%  Lanes: %d  GOMEMLIMIT: %dGiB\n\n",
		*workers, *actors, *actorType, *reqPct, lanes, *memlimit)

	// --- Stats ---

	var sent, requested, replies, errors atomic.Int64

	// --- Workers: tight loops matching the real loadtest ---

	stop := make(chan struct{})
	var wg sync.WaitGroup

	for range *workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}

				ref := theatre.NewRef(*actorType, actorIDs[rand.IntN(poolSize)])

				if rand.Float64() < sendThreshold {
					if err := h.Send(ref, "ping"); err != nil {
						errors.Add(1)
					} else {
						sent.Add(1)
					}
				} else {
					requested.Add(1)
					if _, err := h.Request(ref, "ping"); err != nil {
						errors.Add(1)
					} else {
						replies.Add(1)
					}
				}
			}
		}()
	}

	// --- Stats printer (every second) ---

	go func() {
		tick := time.NewTicker(time.Second)
		defer tick.Stop()
		var prevSent, prevReq, prevReply, prevErr int64
		for {
			select {
			case <-stop:
				return
			case <-tick.C:
				s := sent.Load()
				r := requested.Load()
				rp := replies.Load()
				e := errors.Load()
				fmt.Printf("  sent: %6d/s  req: %5d/s  replies: %5d/s  errors: %4d/s  (total: %d)\n",
					s-prevSent, r-prevReq, rp-prevReply, e-prevErr, s+r)
				prevSent, prevReq, prevReply, prevErr = s, r, rp, e
			}
		}
	}()

	// --- Wait for Ctrl+C ---

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	<-sig

	fmt.Println("\nStopping...")
	close(stop)
	wg.Wait()
	h.Stop()
	t.Stop()
	fmt.Printf("Done. Sent: %d  Requested: %d  Replies: %d  Errors: %d\n",
		sent.Load(), requested.Load(), replies.Load(), errors.Load())
}
