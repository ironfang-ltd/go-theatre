// transport-demo starts two transport nodes on localhost and demonstrates
// message forwarding with request/reply correlation.
//
// Run:  go run ./cmd/transport-demo
package main

import (
	"fmt"
	"log"
	"time"

	"github.com/ironfang-ltd/go-theatre"
)

func main() {
	replyCh := make(chan *theatre.ActorForwardReply, 1)

	// Transport B will be assigned after creation; the closure captures the variable.
	var tB *theatre.Transport

	// --- Handler A: print any replies it receives ---
	handlerA := func(from string, env theatre.TransportEnvelope) {
		switch msg := env.Payload.(type) {
		case *theatre.ActorForwardReply:
			fmt.Printf("[host-a] received reply  ReplyID=%d  Body=%v  Error=%q\n",
				msg.ReplyID, msg.Body, msg.Error)
			replyCh <- msg
		default:
			fmt.Printf("[host-a] unexpected message tag=%d from %s\n", env.Tag, from)
		}
	}

	// --- Handler B: echo back an ActorForwardReply for every ActorForward ---
	handlerB := func(from string, env theatre.TransportEnvelope) {
		switch msg := env.Payload.(type) {
		case *theatre.ActorForward:
			fmt.Printf("[host-b] received forward  ActorType=%s  ActorID=%s  ReplyID=%d  Body=%v\n",
				msg.ActorType, msg.ActorID, msg.ReplyID, msg.Body)

			replyEnv, err := theatre.Envelope(theatre.ActorForwardReply{
				ReplyID: msg.ReplyID,
				Body:    fmt.Sprintf("hello back, you said %q", msg.Body),
			})
			if err != nil {
				log.Printf("[host-b] envelope error: %v", err)
				return
			}
			if err := tB.SendTo(from, "", replyEnv); err != nil {
				log.Printf("[host-b] reply error: %v", err)
			}
		default:
			fmt.Printf("[host-b] unexpected message tag=%d from %s\n", env.Tag, from)
		}
	}

	// --- Start transports ---
	tA, err := theatre.NewTransport("host-a", "127.0.0.1:0", handlerA)
	if err != nil {
		log.Fatalf("NewTransport A: %v", err)
	}
	tA.Start()
	defer tA.Stop()

	tB, err = theatre.NewTransport("host-b", "127.0.0.1:0", handlerB)
	if err != nil {
		log.Fatalf("NewTransport B: %v", err)
	}
	tB.Start()
	defer tB.Stop()

	fmt.Printf("host-a listening on %s\n", tA.Addr())
	fmt.Printf("host-b listening on %s\n", tB.Addr())

	// --- Send a forward from A → B ---
	fmt.Println("\n--- Sending ActorForward from host-a to host-b ---")
	fwdEnv, err := theatre.Envelope(theatre.ActorForward{
		ActorType:    "greeter",
		ActorID:      "1",
		Body:         "hello from host-a",
		ReplyID:      42,
		SenderHostID: "host-a",
	})
	if err != nil {
		log.Fatalf("Envelope: %v", err)
	}
	if err := tA.SendTo("host-b", tB.Addr(), fwdEnv); err != nil {
		log.Fatalf("SendTo: %v", err)
	}

	// --- Wait for the correlated reply ---
	select {
	case reply := <-replyCh:
		fmt.Println("\n--- Correlation check ---")
		if reply.ReplyID == 42 {
			fmt.Println("OK: ReplyID matches (42). Request correlation verified.")
		} else {
			fmt.Printf("FAIL: ReplyID mismatch: got %d, want 42\n", reply.ReplyID)
		}
	case <-time.After(3 * time.Second):
		log.Fatal("timeout waiting for reply")
	}

	fmt.Println("\nDemo complete.")
}
