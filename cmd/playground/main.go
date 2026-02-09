// playground spins up 3 standalone hosts, each with an echo actor,
// sends a few messages and requests, then blocks so you can poke
// around the admin endpoints.
//
// Run:  go run ./cmd/playground
//
// Admin endpoints (per host):
//
//	GET /cluster/status        — host state, metrics, registered types
//	GET /cluster/local-actor?type=echo&id=1  — actor status
//	GET /debug/vars            — expvar metrics
package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/ironfang-ltd/go-theatre"
)

// echoReceiver replies with whatever it receives.
type echoReceiver struct {
	name string
}

func (e *echoReceiver) Receive(ctx *theatre.Context) error {
	switch msg := ctx.Message.(type) {
	case theatre.Initialize:
		fmt.Printf("  [%s/%s] initialized\n", e.name, ctx.ActorRef.ID)
	case theatre.Shutdown:
		fmt.Printf("  [%s/%s] shutting down\n", e.name, ctx.ActorRef.ID)
	case string:
		fmt.Printf("  [%s/%s] received: %q\n", e.name, ctx.ActorRef.ID, msg)
		ctx.Reply(fmt.Sprintf("echo from %s: %s", e.name, msg))
	default:
		fmt.Printf("  [%s/%s] received: %T %v\n", e.name, ctx.ActorRef.ID, msg, msg)
	}
	return nil
}

func main() {
	const numHosts = 3

	type hostInfo struct {
		host      *theatre.Host
		adminAddr string
	}

	hosts := make([]hostInfo, numHosts)

	for i := range hosts {
		adminAddr := fmt.Sprintf("127.0.0.1:%d", 9090+i)
		name := fmt.Sprintf("host-%d", i+1)

		h := theatre.NewHost(
			theatre.WithAdminAddr(adminAddr),
			theatre.WithIdleTimeout(5*time.Minute),
		)

		h.RegisterActor("echo", func() theatre.Receiver {
			return &echoReceiver{name: name}
		})

		hosts[i] = hostInfo{host: h, adminAddr: adminAddr}
	}

	// Start all hosts.
	for i, hi := range hosts {
		hi.host.Start()
		fmt.Printf("host-%d started  admin=http://%s\n", i+1, hi.adminAddr)
	}

	fmt.Println()

	// Send some fire-and-forget messages.
	fmt.Println("--- Sending messages ---")
	for i, hi := range hosts {
		ref := theatre.NewRef("echo", fmt.Sprintf("%d", i+1))
		if err := hi.host.Send(ref, fmt.Sprintf("hello from host-%d", i+1)); err != nil {
			log.Printf("send error: %v", err)
		}
	}

	time.Sleep(200 * time.Millisecond)
	fmt.Println()

	// Send request/reply across hosts.
	fmt.Println("--- Sending requests ---")
	var wg sync.WaitGroup
	for i, hi := range hosts {
		wg.Add(1)
		go func(idx int, h *theatre.Host) {
			defer wg.Done()
			ref := theatre.NewRef("echo", "1")
			resp, err := h.Request(ref, fmt.Sprintf("request from host-%d", idx+1))
			if err != nil {
				fmt.Printf("  host-%d request error: %v\n", idx+1, err)
				return
			}
			fmt.Printf("  host-%d got reply: %v\n", idx+1, resp)
		}(i, hi.host)
	}
	wg.Wait()

	fmt.Println()
	fmt.Println("--- Hosts running. Try these endpoints: ---")
	for i, hi := range hosts {
		fmt.Printf("  host-%d:\n", i+1)
		fmt.Printf("    curl http://%s/cluster/status\n", hi.adminAddr)
		fmt.Printf("    curl http://%s/cluster/local-actor?type=echo&id=%d\n", hi.adminAddr, i+1)
		fmt.Printf("    curl http://%s/debug/vars\n", hi.adminAddr)
	}
	fmt.Println()
	fmt.Println("Press Ctrl+C to stop.")

	// Block until interrupt.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	<-sig

	fmt.Println("\nShutting down...")
	for i := len(hosts) - 1; i >= 0; i-- {
		hosts[i].host.Stop()
		fmt.Printf("host-%d stopped\n", i+1)
	}
}
