package theatre

import (
	"log/slog"
	"strconv"
	"sync"
	"testing"
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
