package theatre

import (
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"testing"
	"time"
)

type TestActor struct {
	wg *sync.WaitGroup
}

type TestActor2 struct {
}

func (a *TestActor) Receive(ctx *Context) error {

	switch msg := ctx.Message.(type) {
	case *TestPing:
		return ctx.Reply(&TestPong{})
	case *TestMessage:
		a.wg.Done()
	case *TestRequestMsg:
		return ctx.Reply(&TestResponseMsg{
			Sequence: msg.Sequence,
		})
	case *TestActorToActorRequestMsg:

		ref := NewRef("test-2", "2")

		res, err := ctx.Request(ref, &TestActorToActorRequestMsg{Sequence: msg.Sequence + 1})
		if err != nil {
			return err
		}

		return ctx.Reply(res)
	}

	return nil
}

func (a *TestActor2) Receive(ctx *Context) error {

	switch msg := ctx.Message.(type) {
	case *TestActorToActorRequestMsg:
		return ctx.Reply(&TestResponseMsg{
			Sequence: msg.Sequence + 1,
		})
	}

	return nil
}

type TestMessage struct{}

type TestPing struct{}

type TestPong struct{}

type TestRequestMsg struct {
	Sequence int
}

type TestActorToActorRequestMsg struct {
	Sequence int
}

type TestResponseMsg struct {
	Sequence int
}

func TestSend(t *testing.T) {

	wg := &sync.WaitGroup{}
	wg.Add(1)

	host := NewHost()

	host.RegisterActor("test", func() Receiver {
		return &TestActor{wg: wg}
	})

	host.Start()
	defer host.Stop()

	ref := NewRef("test", "1")

	err := host.Send(ref, &TestMessage{})
	if err != nil {
		t.Fatal(err)
	}

	wg.Wait()
}

func TestSendMultiple(t *testing.T) {

	wg := &sync.WaitGroup{}
	wg.Add(10)

	host := NewHost()

	host.RegisterActor("test", func() Receiver {
		return &TestActor{wg: wg}
	})

	host.Start()
	defer host.Stop()

	ref := NewRef("test", "1")

	for i := 0; i < 10; i++ {
		err := host.Send(ref, &TestMessage{})
		if err != nil {
			t.Fatal(err)
		}
	}

	wg.Wait()
}

func TestRequest(t *testing.T) {

	host := NewHost()

	host.RegisterActor("test", func() Receiver {
		return &TestActor{wg: &sync.WaitGroup{}}
	})

	host.Start()
	defer host.Stop()

	ref := NewRef("test", "1")

	res, err := host.Request(ref, &TestRequestMsg{Sequence: 1})
	if err != nil {
		t.Fatal(err)
	}

	if res == nil {
		t.Fatal("response is nil")
	}

	if res.(*TestResponseMsg).Sequence != 1 {
		t.Fatal("response sequence is not 1")
	}
}

func TestActorToActor(t *testing.T) {

	host := NewHost()

	host.RegisterActor("test", func() Receiver {
		return &TestActor{wg: &sync.WaitGroup{}}
	})

	host.RegisterActor("test-2", func() Receiver {
		return &TestActor2{}
	})

	host.Start()
	defer host.Stop()

	ref := NewRef("test", "1")

	res, err := host.Request(ref, &TestActorToActorRequestMsg{Sequence: 1})
	if err != nil {
		t.Fatal(err)
	}

	if res == nil {
		t.Fatal("response is nil")
	}

	if res.(*TestResponseMsg).Sequence != 3 {
		t.Fatalf("response sequence is not %d, expected 3", res.(*TestResponseMsg).Sequence)
	}
}

func TestStop(t *testing.T) {

	wg := &sync.WaitGroup{}
	wg.Add(10)

	host := NewHost()

	host.RegisterActor("test", func() Receiver {
		return &TestActor{wg: wg}
	})

	host.Start()

	for i := 0; i < 10; i++ {
		ref := NewRef("test", strconv.Itoa(i))
		err := host.Send(ref, &TestMessage{})
		if err != nil {
			t.Fatal(err)
		}
	}

	wg.Wait()

	host.Stop()

	host.actors.mu.RLock()
	defer host.actors.mu.RUnlock()

	if len(host.actors.actors) != 0 {
		t.Fatal("actors not removed")
	}
}

// test actors for error/panic scenarios

type TestPanicErrorActor struct{}

func (a *TestPanicErrorActor) Receive(ctx *Context) error {
	panic(fmt.Errorf("error panic"))
}

type TestPanicStringActor struct{}

func (a *TestPanicStringActor) Receive(ctx *Context) error {
	panic("string panic")
}

type TestPanicIntActor struct{}

func (a *TestPanicIntActor) Receive(ctx *Context) error {
	panic(42)
}

type TestErrorActor struct{}

var errTestActor = fmt.Errorf("test actor error")

func (a *TestErrorActor) Receive(ctx *Context) error {
	return errTestActor
}

func TestRequest_PanicWithError(t *testing.T) {

	host := NewHost()

	host.RegisterActor("panic-error", func() Receiver {
		return &TestPanicErrorActor{}
	})

	host.Start()
	defer host.Stop()

	ref := NewRef("panic-error", "1")

	_, err := host.Request(ref, &TestPing{})
	if err == nil {
		t.Fatal("expected error from panicking actor")
	}
	if err.Error() != "error panic" {
		t.Fatalf("expected 'error panic', got '%s'", err.Error())
	}
}

func TestRequest_PanicWithString(t *testing.T) {

	host := NewHost()

	host.RegisterActor("panic-string", func() Receiver {
		return &TestPanicStringActor{}
	})

	host.Start()
	defer host.Stop()

	ref := NewRef("panic-string", "1")

	_, err := host.Request(ref, &TestPing{})
	if err == nil {
		t.Fatal("expected error from panicking actor")
	}
	if err.Error() != "panic: string panic" {
		t.Fatalf("expected 'panic: string panic', got '%s'", err.Error())
	}
}

func TestRequest_PanicWithInt(t *testing.T) {

	host := NewHost()

	host.RegisterActor("panic-int", func() Receiver {
		return &TestPanicIntActor{}
	})

	host.Start()
	defer host.Stop()

	ref := NewRef("panic-int", "1")

	_, err := host.Request(ref, &TestPing{})
	if err == nil {
		t.Fatal("expected error from panicking actor")
	}
	if err.Error() != "panic: 42" {
		t.Fatalf("expected 'panic: 42', got '%s'", err.Error())
	}
}

func TestRequest_ReceiveError(t *testing.T) {

	host := NewHost()

	host.RegisterActor("error", func() Receiver {
		return &TestErrorActor{}
	})

	host.Start()
	defer host.Stop()

	ref := NewRef("error", "1")

	_, err := host.Request(ref, &TestPing{})
	if err == nil {
		t.Fatal("expected error from actor")
	}
	if !errors.Is(err, errTestActor) {
		t.Fatalf("expected errTestActor, got %v", err)
	}
}

func TestRequest_UnregisteredActor(t *testing.T) {

	host := NewHost()
	host.Start()
	defer host.Stop()

	ref := NewRef("nonexistent", "1")

	// manually trigger expiry with a short deadline
	// since we can't change the 5s timeout, directly test that
	// the request manager handles the missing actor
	req := host.requests.Create(ref)

	// simulate immediate expiry
	host.requests.mu.Lock()
	req.SentAt = req.SentAt.Add(-10 * time.Second)
	host.requests.mu.Unlock()

	host.requests.RemoveExpired()

	res := <-req.Response
	if res.Error != ErrRequestTimeout {
		t.Fatalf("expected ErrRequestTimeout, got %v", res.Error)
	}
}

func TestRequest_MultipleSequential(t *testing.T) {

	host := NewHost()

	host.RegisterActor("test", func() Receiver {
		return &TestActor{wg: &sync.WaitGroup{}}
	})

	host.Start()
	defer host.Stop()

	ref := NewRef("test", "1")

	for i := 0; i < 100; i++ {
		res, err := host.Request(ref, &TestRequestMsg{Sequence: i})
		if err != nil {
			t.Fatalf("request %d failed: %v", i, err)
		}
		if res.(*TestResponseMsg).Sequence != i {
			t.Fatalf("request %d: expected sequence %d, got %d", i, i, res.(*TestResponseMsg).Sequence)
		}
	}
}

type TestInitActor struct {
	initialized bool
	messages    []interface{}
	wg          *sync.WaitGroup
}

func (a *TestInitActor) Receive(ctx *Context) error {
	switch ctx.Message.(type) {
	case Initialize:
		a.initialized = true
	case Shutdown:
		// handled by framework
	default:
		a.messages = append(a.messages, ctx.Message)
		a.wg.Done()
	}
	return nil
}

func TestInitializeMessage(t *testing.T) {

	wg := &sync.WaitGroup{}
	wg.Add(1)

	var actor *TestInitActor

	host := NewHost()

	host.RegisterActor("init-test", func() Receiver {
		actor = &TestInitActor{wg: wg}
		return actor
	})

	host.Start()
	defer host.Stop()

	ref := NewRef("init-test", "1")

	err := host.Send(ref, &TestMessage{})
	if err != nil {
		t.Fatal(err)
	}

	wg.Wait()

	if !actor.initialized {
		t.Fatal("actor was not initialized before receiving messages")
	}
	if len(actor.messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(actor.messages))
	}
}

func TestInitializeBeforeRequest(t *testing.T) {

	var initialized bool

	host := NewHost()

	host.RegisterActor("init-req-test", func() Receiver {
		return &testInitReqActor{initialized: &initialized}
	})

	host.Start()
	defer host.Stop()

	ref := NewRef("init-req-test", "1")

	res, err := host.Request(ref, &TestPing{})
	if err != nil {
		t.Fatal(err)
	}

	if !initialized {
		t.Fatal("actor was not initialized before handling request")
	}

	if _, ok := res.(*TestPong); !ok {
		t.Fatalf("expected *TestPong, got %T", res)
	}
}

type testInitReqActor struct {
	initialized *bool
}

func (a *testInitReqActor) Receive(ctx *Context) error {
	switch ctx.Message.(type) {
	case Initialize:
		*a.initialized = true
	case *TestPing:
		if !*a.initialized {
			return fmt.Errorf("not initialized")
		}
		return ctx.Reply(&TestPong{})
	}
	return nil
}

func BenchmarkHost_Request(b *testing.B) {

	slog.SetLogLoggerLevel(slog.LevelError)

	host := NewHost()

	host.RegisterActor("test", func() Receiver {
		return &TestActor{wg: &sync.WaitGroup{}}
	})

	host.Start()

	ref := NewRef("test", "1")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := host.Request(ref, &TestPing{})
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()

	host.Stop()

	b.Logf("processed %d requests in %s", b.N, b.Elapsed().String())
}
