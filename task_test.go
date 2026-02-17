package theatre

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// taskReceiver collects TaskCompleted and TaskFailed messages for assertions.
type taskReceiver struct {
	completed chan TaskCompleted
	failed    chan TaskFailed
	started   chan struct{}
	spawnFn   func(ctx *Context) // called on Initialize to spawn tasks
	otherMsg  chan interface{}    // collects non-task messages
}

func newTaskReceiver() *taskReceiver {
	return &taskReceiver{
		completed: make(chan TaskCompleted, 10),
		failed:    make(chan TaskFailed, 10),
		started:   make(chan struct{}),
		otherMsg:  make(chan interface{}, 10),
	}
}

func (r *taskReceiver) Receive(ctx *Context) error {
	switch msg := ctx.Message.(type) {
	case Initialize:
		close(r.started)
		if r.spawnFn != nil {
			r.spawnFn(ctx)
		}
	case TaskCompleted:
		r.completed <- msg
	case TaskFailed:
		r.failed <- msg
	case Shutdown:
	default:
		r.otherMsg <- msg
	}
	return nil
}

func TestSpawnTask_Completes(t *testing.T) {
	h := NewHost()
	r := newTaskReceiver()
	r.spawnFn = func(ctx *Context) {
		_, err := ctx.SpawnTask(func(tc *TaskContext) (any, error) {
			return "hello", nil
		})
		if err != nil {
			t.Errorf("SpawnTask error: %v", err)
		}
	}
	h.RegisterActor("test", func() Receiver { return r })
	h.Start()
	defer h.Stop()

	h.Send(Ref{Type: "test", ID: "1"}, "trigger")
	<-r.started

	select {
	case tc := <-r.completed:
		if tc.Result != "hello" {
			t.Errorf("expected result 'hello', got %v", tc.Result)
		}
		if tc.TaskID != 1 {
			t.Errorf("expected task ID 1, got %d", tc.TaskID)
		}
	case tf := <-r.failed:
		t.Fatalf("unexpected TaskFailed: %v", tf.Error)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for TaskCompleted")
	}
}

func TestSpawnTask_Fails(t *testing.T) {
	h := NewHost()
	r := newTaskReceiver()
	testErr := errors.New("task error")
	r.spawnFn = func(ctx *Context) {
		_, err := ctx.SpawnTask(func(tc *TaskContext) (any, error) {
			return nil, testErr
		})
		if err != nil {
			t.Errorf("SpawnTask error: %v", err)
		}
	}
	h.RegisterActor("test", func() Receiver { return r })
	h.Start()
	defer h.Stop()

	h.Send(Ref{Type: "test", ID: "1"}, "trigger")
	<-r.started

	select {
	case tf := <-r.failed:
		if tf.Error != testErr {
			t.Errorf("expected error %v, got %v", testErr, tf.Error)
		}
		if tf.TaskID != 1 {
			t.Errorf("expected task ID 1, got %d", tf.TaskID)
		}
	case tc := <-r.completed:
		t.Fatalf("unexpected TaskCompleted: %v", tc.Result)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for TaskFailed")
	}
}

func TestSpawnTask_PanicRecovery(t *testing.T) {
	h := NewHost()
	r := newTaskReceiver()
	r.spawnFn = func(ctx *Context) {
		ctx.SpawnTask(func(tc *TaskContext) (any, error) {
			panic("boom")
		})
	}
	h.RegisterActor("test", func() Receiver { return r })
	h.Start()
	defer h.Stop()

	h.Send(Ref{Type: "test", ID: "1"}, "trigger")
	<-r.started

	select {
	case tf := <-r.failed:
		if tf.Error == nil {
			t.Fatal("expected non-nil error from panic")
		}
		if tf.Error.Error() != "task panic: boom" {
			t.Errorf("unexpected error message: %v", tf.Error)
		}
	case <-r.completed:
		t.Fatal("unexpected TaskCompleted after panic")
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for TaskFailed")
	}

	// Drain the original "trigger" message that went to otherMsg.
	select {
	case <-r.otherMsg:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout draining trigger message")
	}

	// Verify the actor is still alive by sending another message.
	h.Send(Ref{Type: "test", ID: "1"}, "after-panic")
	select {
	case msg := <-r.otherMsg:
		if msg != "after-panic" {
			t.Errorf("expected 'after-panic', got %v", msg)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout: actor did not process message after panic")
	}
}

func TestSpawnTask_MaxExceeded(t *testing.T) {
	h := NewHost(WithMaxTasksPerActor(2))

	block := make(chan struct{})
	spawnErrCh := make(chan error, 1)

	r := newTaskReceiver()
	r.spawnFn = func(ctx *Context) {
		// Spawn 2 tasks (fill the limit).
		ctx.SpawnTask(func(tc *TaskContext) (any, error) {
			<-block
			return nil, nil
		})
		ctx.SpawnTask(func(tc *TaskContext) (any, error) {
			<-block
			return nil, nil
		})
		// Third should fail.
		_, err := ctx.SpawnTask(func(tc *TaskContext) (any, error) {
			return nil, nil
		})
		spawnErrCh <- err
	}
	h.RegisterActor("test", func() Receiver { return r })
	h.Start()
	defer func() {
		close(block)
		h.Stop()
	}()

	h.Send(Ref{Type: "test", ID: "1"}, "trigger")
	<-r.started

	select {
	case spawnErr := <-spawnErrCh:
		if !errors.Is(spawnErr, ErrMaxTasksExceeded) {
			t.Fatalf("expected ErrMaxTasksExceeded, got %v", spawnErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for spawn error")
	}
}

func TestSpawnTask_PreventsIdleRemoval(t *testing.T) {
	h := NewHost(
		WithIdleTimeout(50*time.Millisecond),
		WithCleanupInterval(25*time.Millisecond),
	)

	taskDone := make(chan struct{})
	r := newTaskReceiver()
	r.spawnFn = func(ctx *Context) {
		ctx.SpawnTask(func(tc *TaskContext) (any, error) {
			<-taskDone
			return "done", nil
		})
	}
	h.RegisterActor("test", func() Receiver { return r })
	h.Start()
	defer h.Stop()

	ref := Ref{Type: "test", ID: "1"}
	h.Send(ref, "trigger")
	<-r.started

	// Wait longer than idle timeout.
	time.Sleep(200 * time.Millisecond)

	// Actor should still exist because a task is running.
	a := h.actors.Lookup(ref)
	if a == nil {
		t.Fatal("actor was removed despite running task")
	}

	// Release the task.
	close(taskDone)

	// Wait for completion message and then idle timeout.
	select {
	case <-r.completed:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for TaskCompleted")
	}

	// Now the actor should be removable by idle cleanup.
	time.Sleep(200 * time.Millisecond)
	a = h.actors.Lookup(ref)
	if a != nil {
		t.Fatal("actor was not removed after task completed and idle timeout")
	}
}

func TestSpawnTask_CancelledOnShutdown(t *testing.T) {
	h := NewHost()

	cancelled := make(chan struct{})
	r := newTaskReceiver()
	r.spawnFn = func(ctx *Context) {
		ctx.SpawnTask(func(tc *TaskContext) (any, error) {
			<-tc.Ctx.Done()
			close(cancelled)
			return nil, tc.Ctx.Err()
		})
	}
	h.RegisterActor("test", func() Receiver { return r })
	h.Start()

	h.Send(Ref{Type: "test", ID: "1"}, "trigger")
	<-r.started
	// Give the task goroutine time to start and block on tc.Ctx.Done().
	time.Sleep(50 * time.Millisecond)

	// Stopping the host shuts down the actor, which cancels the actor context,
	// which cascades to the task context.
	h.Stop()

	select {
	case <-cancelled:
		// Task was cancelled as expected.
	case <-time.After(5 * time.Second):
		t.Fatal("timeout: task context was not cancelled on shutdown")
	}
}

func TestSpawnTask_Timeout(t *testing.T) {
	h := NewHost(WithMaxTaskDuration(100 * time.Millisecond))

	cancelled := make(chan struct{})
	r := newTaskReceiver()
	r.spawnFn = func(ctx *Context) {
		ctx.SpawnTask(func(tc *TaskContext) (any, error) {
			<-tc.Ctx.Done()
			close(cancelled)
			return nil, tc.Ctx.Err()
		})
	}
	h.RegisterActor("test", func() Receiver { return r })
	h.Start()
	defer h.Stop()

	h.Send(Ref{Type: "test", ID: "1"}, "trigger")
	<-r.started

	select {
	case <-cancelled:
		// Task was cancelled by timeout.
	case <-time.After(5 * time.Second):
		t.Fatal("timeout: task was not cancelled by WithMaxTaskDuration")
	}

	// Should receive a TaskFailed.
	select {
	case tf := <-r.failed:
		if tf.Error == nil {
			t.Fatal("expected non-nil error from timeout")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for TaskFailed after task timeout")
	}
}

func TestSpawnTask_SendToOtherActor(t *testing.T) {
	h := NewHost()

	received := make(chan string, 1)

	// Target actor that receives the message from the task.
	h.RegisterActor("target", func() Receiver {
		return ReceiverFunc(func(ctx *Context) error {
			if s, ok := ctx.Message.(string); ok {
				received <- s
			}
			return nil
		})
	})

	r := newTaskReceiver()
	r.spawnFn = func(ctx *Context) {
		ctx.SpawnTask(func(tc *TaskContext) (any, error) {
			tc.Send(Ref{Type: "target", ID: "t1"}, "from-task")
			return nil, nil
		})
	}
	h.RegisterActor("test", func() Receiver { return r })
	h.Start()
	defer h.Stop()

	h.Send(Ref{Type: "test", ID: "1"}, "trigger")
	<-r.started

	select {
	case msg := <-received:
		if msg != "from-task" {
			t.Errorf("expected 'from-task', got %q", msg)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout: target actor did not receive message from task")
	}
}

func TestSpawnTask_RequestFromTask(t *testing.T) {
	h := NewHost()

	// Echo actor that replies with the message it received.
	h.RegisterActor("echo", func() Receiver {
		return ReceiverFunc(func(ctx *Context) error {
			if s, ok := ctx.Message.(string); ok {
				ctx.Reply("echo:" + s)
			}
			return nil
		})
	})

	r := newTaskReceiver()
	r.spawnFn = func(ctx *Context) {
		ctx.SpawnTask(func(tc *TaskContext) (any, error) {
			resp, err := tc.Request(Ref{Type: "echo", ID: "e1"}, "ping")
			if err != nil {
				return nil, err
			}
			return resp, nil
		})
	}
	h.RegisterActor("test", func() Receiver { return r })
	h.Start()
	defer h.Stop()

	h.Send(Ref{Type: "test", ID: "1"}, "trigger")
	<-r.started

	select {
	case tc := <-r.completed:
		if tc.Result != "echo:ping" {
			t.Errorf("expected 'echo:ping', got %v", tc.Result)
		}
	case tf := <-r.failed:
		t.Fatalf("unexpected TaskFailed: %v", tf.Error)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for TaskCompleted from request")
	}
}

func TestSpawnTask_Metrics(t *testing.T) {
	h := NewHost(WithMaxTasksPerActor(2))

	block := make(chan struct{})
	spawned := make(chan struct{})

	r := newTaskReceiver()
	r.spawnFn = func(ctx *Context) {
		// One success, one failure.
		ctx.SpawnTask(func(tc *TaskContext) (any, error) {
			return "ok", nil
		})
		ctx.SpawnTask(func(tc *TaskContext) (any, error) {
			return nil, errors.New("fail")
		})
		// Third exceeds limit.
		ctx.SpawnTask(func(tc *TaskContext) (any, error) {
			<-block
			return nil, nil
		})
		close(spawned)
	}
	h.RegisterActor("test", func() Receiver { return r })
	h.Start()
	defer h.Stop()

	h.Send(Ref{Type: "test", ID: "1"}, "trigger")
	<-r.started
	<-spawned

	// Wait for both tasks to complete.
	var gotCompleted, gotFailed bool
	for i := 0; i < 2; i++ {
		select {
		case <-r.completed:
			gotCompleted = true
		case <-r.failed:
			gotFailed = true
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for task results")
		}
	}
	if !gotCompleted || !gotFailed {
		t.Fatalf("expected both completed and failed, got completed=%v failed=%v", gotCompleted, gotFailed)
	}

	snap := h.Metrics().Snapshot()
	if snap["tasks_spawned"] != 2 {
		t.Errorf("expected tasks_spawned=2, got %d", snap["tasks_spawned"])
	}
	if snap["tasks_completed"] != 1 {
		t.Errorf("expected tasks_completed=1, got %d", snap["tasks_completed"])
	}
	if snap["tasks_failed"] != 1 {
		t.Errorf("expected tasks_failed=1, got %d", snap["tasks_failed"])
	}
}

// Verify that actors track running tasks atomically.
func TestSpawnTask_RunningTasksCounter(t *testing.T) {
	h := NewHost(WithMaxTasksPerActor(10))

	block := make(chan struct{})
	var running atomic.Int32
	var peak atomic.Int32

	r := newTaskReceiver()
	r.spawnFn = func(ctx *Context) {
		for i := 0; i < 5; i++ {
			ctx.SpawnTask(func(tc *TaskContext) (any, error) {
				cur := running.Add(1)
				for {
					old := peak.Load()
					if int32(cur) <= old || peak.CompareAndSwap(old, int32(cur)) {
						break
					}
				}
				<-block
				running.Add(-1)
				return nil, nil
			})
		}
	}
	h.RegisterActor("test", func() Receiver { return r })
	h.Start()
	defer func() {
		close(block)
		h.Stop()
	}()

	h.Send(Ref{Type: "test", ID: "1"}, "trigger")
	<-r.started

	// Give goroutines time to start.
	time.Sleep(100 * time.Millisecond)

	if p := peak.Load(); p != 5 {
		t.Errorf("expected 5 concurrent tasks, peak was %d", p)
	}

	ref := Ref{Type: "test", ID: "1"}
	a := h.actors.Lookup(ref)
	if a == nil {
		t.Fatal("actor not found")
	}
	if rt := a.runningTasks.Load(); rt != 5 {
		t.Errorf("expected runningTasks=5, got %d", rt)
	}
}
