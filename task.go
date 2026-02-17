package theatre

import (
	"context"
	"fmt"
	"runtime/debug"
	"time"
)

// ErrMaxTasksExceeded is returned when SpawnTask is called but the actor
// has already reached its maximum number of concurrent background tasks.
var ErrMaxTasksExceeded = fmt.Errorf("max tasks exceeded")

// ErrTaskAlreadyRunning is returned when SpawnNamedTask is called with a
// name that already has a running task on this actor.
var ErrTaskAlreadyRunning = fmt.Errorf("task already running")

// taskInfo tracks metadata for a running background task.
type taskInfo struct {
	ID        int64
	Name      string
	StartedAt time.Time
}

// TaskCompleted is delivered to the owning actor when a background task
// returns a nil error.
type TaskCompleted struct {
	TaskID int64
	Name   string
	Result interface{}
}

// TaskFailed is delivered to the owning actor when a background task
// returns an error or panics.
type TaskFailed struct {
	TaskID int64
	Name   string
	Error  error
}

// TaskContext is passed to background task functions spawned via SpawnTask.
// It provides a cancellable context and the ability to send messages to
// other actors while running outside the actor's receive loop.
type TaskContext struct {
	// Ctx is derived from the actor's context with the task timeout applied.
	// Check Ctx.Done() to detect cancellation from actor shutdown, host
	// freeze, or task timeout.
	Ctx context.Context

	host *Host
	ref  Ref
}

// Send delivers a fire-and-forget message to the actor identified by ref.
// Returns an error if the task context has been cancelled.
func (tc *TaskContext) Send(ref Ref, body interface{}) error {
	if err := tc.Ctx.Err(); err != nil {
		return err
	}
	tc.host.sendInternal(OutboxMessage{
		RecipientRef: ref,
		Body:         body,
	})
	return nil
}

// Request sends a message to the actor identified by ref and blocks until
// a reply is received or the request timeout expires. Returns an error if
// the task context has been cancelled.
func (tc *TaskContext) Request(ref Ref, body interface{}) (any, error) {
	if err := tc.Ctx.Err(); err != nil {
		return nil, err
	}
	return tc.host.requestInternal(ref, body)
}

// Self returns the Ref of the actor that spawned this task.
func (tc *TaskContext) Self() Ref {
	return tc.ref
}

// spawnTask launches a background goroutine for the given function, linked
// to this actor's lifecycle. When name is non-empty, the task is deduplicated:
// if a task with the same name is already running, ErrTaskAlreadyRunning is
// returned. Returns a monotonic task ID or an error.
func (a *Actor) spawnTask(name string, fn func(*TaskContext) (any, error)) (int64, error) {
	// Dedup check for named tasks.
	if name != "" {
		alreadyRunning := false
		a.tasks.Range(func(_, v any) bool {
			if v.(*taskInfo).Name == name {
				alreadyRunning = true
				return false
			}
			return true
		})
		if alreadyRunning {
			return 0, ErrTaskAlreadyRunning
		}
	}

	// CAS loop to atomically check and increment running task count.
	for {
		current := a.runningTasks.Load()
		if int(current) >= a.maxTasks {
			return 0, ErrMaxTasksExceeded
		}
		if a.runningTasks.CompareAndSwap(current, current+1) {
			break
		}
	}

	taskID := a.taskSeq.Add(1)
	taskCtx, taskCancel := context.WithTimeout(a.actorCtx, a.maxTaskDur)

	tc := &TaskContext{
		Ctx:  taskCtx,
		host: a.host,
		ref:  a.ref,
	}

	a.tasks.Store(taskID, &taskInfo{ID: taskID, Name: name, StartedAt: time.Now()})
	a.host.metrics.TasksSpawned.Add(1)

	go func() {
		defer taskCancel()
		defer a.tasks.Delete(taskID)
		defer a.runningTasks.Add(-1)

		var result any
		var err error

		func() {
			defer func() {
				if r := recover(); r != nil {
					debug.PrintStack()
					if e, ok := r.(error); ok {
						err = e
					} else {
						err = fmt.Errorf("task panic: %v", r)
					}
				}
			}()
			result, err = fn(tc)
		}()

		if err != nil {
			a.host.metrics.TasksFailed.Add(1)
			a.Send(InboxMessage{
				RecipientRef: a.ref,
				Body:         TaskFailed{TaskID: taskID, Name: name, Error: err},
			})
		} else {
			a.host.metrics.TasksCompleted.Add(1)
			a.Send(InboxMessage{
				RecipientRef: a.ref,
				Body:         TaskCompleted{TaskID: taskID, Name: name, Result: result},
			})
		}
	}()

	return taskID, nil
}

// TaskSnapshot returns metadata for all currently running tasks.
func (a *Actor) TaskSnapshot() []taskInfo {
	var out []taskInfo
	a.tasks.Range(func(_, v any) bool {
		out = append(out, *v.(*taskInfo))
		return true
	})
	return out
}

