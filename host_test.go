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

func (a *TestActor) Receive(ctx *Context) error {

	switch ctx.Message.(type) {
	case *TestPing:
		ctx.Reply(&TestPong{})
	case *TestMessage:
		a.wg.Done()
	case *TestRequestMsg:
		ctx.Reply(&TestResponseMsg{
			Sequence: ctx.Message.(*TestRequestMsg).Sequence,
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

	host.Send(ref, &TestMessage{})

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
		host.Send(ref, &TestMessage{})
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
		host.Send(ref, &TestMessage{})
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
