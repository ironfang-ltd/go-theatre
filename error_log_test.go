package theatre

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestErrorLog_Empty(t *testing.T) {
	el := newErrorLog(256)
	got := el.Recent(10)
	if len(got) != 0 {
		t.Fatalf("expected empty slice, got %d entries", len(got))
	}
	if el.Total() != 0 {
		t.Fatalf("expected total=0, got %d", el.Total())
	}
}

func TestErrorLog_Recent_NewestFirst(t *testing.T) {
	el := newErrorLog(256)

	for i := range 5 {
		el.Record(ErrorEntry{
			Time:    time.Now(),
			Level:   "error",
			Source:  "test",
			Message: string(rune('A' + i)),
		})
	}

	got := el.Recent(5)
	if len(got) != 5 {
		t.Fatalf("expected 5 entries, got %d", len(got))
	}

	// Newest first: E, D, C, B, A
	for i, want := range []string{"E", "D", "C", "B", "A"} {
		if got[i].Message != want {
			t.Errorf("got[%d].Message = %q, want %q", i, got[i].Message, want)
		}
	}
}

func TestErrorLog_WrapAround(t *testing.T) {
	el := newErrorLog(4) // small capacity

	// Write 7 entries (wraps around).
	for i := range 7 {
		el.Record(ErrorEntry{
			Time:    time.Now(),
			Level:   "error",
			Source:  "test",
			Message: string(rune('A' + i)),
		})
	}

	if el.Total() != 7 {
		t.Fatalf("expected total=7, got %d", el.Total())
	}

	// Should only return 4 (capacity) even if we ask for more.
	got := el.Recent(10)
	if len(got) != 4 {
		t.Fatalf("expected 4 entries, got %d", len(got))
	}

	// Newest first: G, F, E, D
	for i, want := range []string{"G", "F", "E", "D"} {
		if got[i].Message != want {
			t.Errorf("got[%d].Message = %q, want %q", i, got[i].Message, want)
		}
	}
}

func TestErrorLog_LimitedRecent(t *testing.T) {
	el := newErrorLog(256)
	for i := range 10 {
		el.Record(ErrorEntry{
			Time:    time.Now(),
			Level:   "warn",
			Source:  "test",
			Message: string(rune('A' + i)),
		})
	}

	got := el.Recent(3)
	if len(got) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(got))
	}

	// Newest 3: J, I, H
	for i, want := range []string{"J", "I", "H"} {
		if got[i].Message != want {
			t.Errorf("got[%d].Message = %q, want %q", i, got[i].Message, want)
		}
	}
}

func TestErrorLog_Concurrent(t *testing.T) {
	el := newErrorLog(64)
	const writers = 8
	const perWriter = 200

	var wg sync.WaitGroup
	wg.Add(writers)
	for w := range writers {
		go func() {
			defer wg.Done()
			for i := range perWriter {
				el.Record(ErrorEntry{
					Time:    time.Now(),
					Level:   "error",
					Source:  fmt.Sprintf("w%d", w),
					Message: fmt.Sprintf("msg-%d", i),
				})
			}
		}()
	}

	// Concurrent readers while writers are active.
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-done:
				return
			default:
				el.Recent(10)
				el.Total()
			}
		}
	}()

	wg.Wait()
	close(done)

	if el.Total() != writers*perWriter {
		t.Errorf("Total = %d, want %d", el.Total(), writers*perWriter)
	}

	got := el.Recent(64)
	if len(got) != 64 {
		t.Errorf("Recent(64) returned %d entries, want 64", len(got))
	}
}
