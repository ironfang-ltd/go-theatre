// playground spins up 3 clustered hosts connected via transport,
// sends messages and cross-host requests, then blocks so you can
// explore the dashboard and admin endpoints.
//
// Run:
//
//	cd web/dashboard && npm run dev   # start Vite on :3000
//	go run ./cmd/playground           # start hosts (dashboard on :9090)
//
// Admin endpoints (per host):
//
//	GET /                      — dashboard (host-1 only, proxied to Vite in dev)
//	GET /cluster/status        — host state, metrics, registered types
//	GET /cluster/hosts         — live cluster members
//	GET /cluster/actors        — all local actors
//	GET /cluster/actor-detail?type=echo&id=1  — actor detail
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
	case time.Time:
		fmt.Printf("  [%s/%s] tick at %s\n", e.name, ctx.ActorRef.ID, msg.Format("15:04:05"))
	default:
		fmt.Printf("  [%s/%s] received: %T %v\n", e.name, ctx.ActorRef.ID, msg, msg)
	}
	return nil
}

// tickReceiver prints the time it receives, then reschedules itself in 5s.
type tickReceiver struct {
	name string
}

func (tr *tickReceiver) Receive(ctx *theatre.Context) error {
	switch msg := ctx.Message.(type) {
	case theatre.Initialize:
		fmt.Printf("  [%s/%s] initialized\n", tr.name, ctx.ActorRef.ID)
	case theatre.Shutdown:
		fmt.Printf("  [%s/%s] shutting down\n", tr.name, ctx.ActorRef.ID)
	case time.Time:
		fmt.Printf("  [%s/%s] tick at %s\n", tr.name, ctx.ActorRef.ID, msg.Format("15:04:05.000"))
		// Reschedule: send the current time again in 5s.
		ctx.SendAfter(ctx.ActorRef, time.Now(), 5*time.Second)
	}
	return nil
}

func main() {
	const numHosts = 3

	type node struct {
		host      *theatre.Host
		transport *theatre.Transport
		hostID    string
		adminAddr string
	}

	nodes := make([]node, numHosts)

	// Phase 1: Create hosts, transports, register actors.
	for i := range nodes {
		adminAddr := fmt.Sprintf("127.0.0.1:%d", 9090+i)
		hostID := fmt.Sprintf("host-%d", i+1)
		name := hostID

		opts := []theatre.Option{
			theatre.WithAdminAddr(adminAddr),
			theatre.WithIdleTimeout(5 * time.Minute),
		}
		if i == 0 {
			opts = append(opts, theatre.WithDashboardDev())
		}
		h := theatre.NewHost(opts...)

		h.RegisterActor("echo", func() theatre.Receiver {
			return &echoReceiver{name: name}
		})
		h.RegisterActor("ticker", func() theatre.Receiver {
			return &tickReceiver{name: name}
		})

		// Create transport (bind to :0 for auto port).
		t, err := theatre.NewTransport(hostID, "127.0.0.1:0", h.HandleTransportMessage)
		if err != nil {
			log.Fatalf("transport %s: %v", hostID, err)
		}
		t.Start()

		nodes[i] = node{
			host:      h,
			transport: t,
			hostID:    hostID,
			adminAddr: adminAddr,
		}
	}

	// Phase 2: Build cluster membership and wire everything up.
	ringMembers := make([]string, numHosts)
	hostInfos := make([]theatre.HostInfo, numHosts)
	for i, n := range nodes {
		ringMembers[i] = n.hostID
		hostInfos[i] = theatre.HostInfo{
			HostID:    n.hostID,
			Address:   n.transport.Addr(),
			AdminAddr: n.adminAddr,
			Epoch:     1,
		}
	}

	for i, n := range nodes {
		cluster := theatre.NewRingOnlyCluster(n.hostID, n.transport.Addr(), 1)
		cluster.Ring().Set(ringMembers)
		cluster.SetHosts(hostInfos)

		n.host.SetTransport(n.transport)
		n.host.SetCluster(cluster)
		nodes[i] = n
	}

	// Phase 3: Start all hosts.
	for _, n := range nodes {
		n.host.Start()
		fmt.Printf("%s started  admin=http://%s  transport=%s\n",
			n.hostID, n.adminAddr, n.transport.Addr())
	}

	fmt.Println()

	// Send some fire-and-forget messages.
	fmt.Println("--- Sending messages ---")
	for i, n := range nodes {
		ref := theatre.NewRef("echo", fmt.Sprintf("%d", i+1))
		if err := n.host.Send(ref, fmt.Sprintf("hello from %s", n.hostID)); err != nil {
			log.Printf("send error: %v", err)
		}
	}

	time.Sleep(200 * time.Millisecond)
	fmt.Println()

	// Schedule a one-shot message on host-1, fires 30s after startup.
	fmt.Println("--- Scheduling messages ---")
	ref1 := theatre.NewRef("echo", "1")
	id1, err := nodes[0].host.SendAfter(ref1, "delayed hello (30s)", 30*time.Second)
	if err != nil {
		log.Printf("SendAfter error: %v", err)
	} else {
		fmt.Printf("  host-1: scheduled one-shot in 30s (id=%d)\n", id1)
	}

	// Schedule a recurring tick on host-1 via self-rescheduling SendAfter.
	tickRef := theatre.NewRef("ticker", "1")
	if _, err := nodes[0].host.SendAfter(tickRef, time.Now(), 5*time.Second); err != nil {
		log.Printf("SendAfter tick error: %v", err)
	} else {
		fmt.Printf("  host-1: recurring tick every 5s (actor ticker/1)\n")
	}

	// Schedule a cron job on host-2, fires every minute.
	cronRef := theatre.NewRef("echo", "cron")
	if id, err := nodes[1].host.SendCron(cronRef, "cron ping", "* * * * *"); err != nil {
		log.Printf("SendCron error: %v", err)
	} else {
		fmt.Printf("  host-2: cron every minute (id=%d, actor echo/cron)\n", id)
	}
	fmt.Println()

	// Send cross-host requests (each host requests echo/1 — the ring
	// determines which host owns it, transport routes the message).
	fmt.Println("--- Sending cross-host requests ---")
	var wg sync.WaitGroup
	for i, n := range nodes {
		wg.Add(1)
		go func(idx int, h *theatre.Host, hostID string) {
			defer wg.Done()
			ref := theatre.NewRef("echo", "1")
			resp, err := h.Request(ref, fmt.Sprintf("request from %s", hostID))
			if err != nil {
				fmt.Printf("  %s request error: %v\n", hostID, err)
				return
			}
			fmt.Printf("  %s got reply: %v\n", hostID, resp)
		}(i, n.host, n.hostID)
	}
	wg.Wait()

	fmt.Println()
	fmt.Println("--- Cluster running. Try these endpoints: ---")
	fmt.Printf("  Dashboard: http://%s/\n", nodes[0].adminAddr)
	fmt.Println()
	for _, n := range nodes {
		fmt.Printf("  %s:\n", n.hostID)
		fmt.Printf("    curl http://%s/cluster/status\n", n.adminAddr)
		fmt.Printf("    curl http://%s/cluster/hosts\n", n.adminAddr)
		fmt.Printf("    curl http://%s/cluster/actors\n", n.adminAddr)
		fmt.Printf("    curl http://%s/debug/vars\n", n.adminAddr)
	}
	fmt.Println()
	fmt.Println("Press Ctrl+C to stop.")

	// Block until interrupt.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	<-sig

	fmt.Println("\nShutting down...")
	for i := len(nodes) - 1; i >= 0; i-- {
		nodes[i].host.Stop()
		nodes[i].transport.Stop()
		fmt.Printf("%s stopped\n", nodes[i].hostID)
	}
}
