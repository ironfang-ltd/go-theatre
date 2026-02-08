package theatre

import (
	"testing"
	"time"
)

func TestRequestManager_CreateAndGet(t *testing.T) {
	rm := NewRequestManager()
	ref := NewRef("test", "1")

	req := rm.Create(ref)

	if req.ID == 0 {
		t.Fatal("expected non-zero request ID")
	}
	if req.To != ref {
		t.Fatalf("expected To=%v, got %v", ref, req.To)
	}

	got := rm.Get(req.ID)
	if got != req {
		t.Fatal("Get returned different request than Create")
	}
}

func TestRequestManager_CreateIncrementsID(t *testing.T) {
	rm := NewRequestManager()
	ref := NewRef("test", "1")

	r1 := rm.Create(ref)
	r2 := rm.Create(ref)

	if r2.ID <= r1.ID {
		t.Fatalf("expected r2.ID > r1.ID, got %d <= %d", r2.ID, r1.ID)
	}
}

func TestRequestManager_GetNonExistent(t *testing.T) {
	rm := NewRequestManager()

	got := rm.Get(999)
	if got != nil {
		t.Fatalf("expected nil for non-existent ID, got %v", got)
	}
}

func TestRequestManager_Remove(t *testing.T) {
	rm := NewRequestManager()
	ref := NewRef("test", "1")

	req := rm.Create(ref)
	id := req.ID

	rm.Remove(id)

	got := rm.Get(id)
	if got != nil {
		t.Fatal("expected nil after Remove")
	}
}

func TestRequestManager_RemoveNonExistent(t *testing.T) {
	rm := NewRequestManager()

	// should not panic
	rm.Remove(999)
}

func TestRequestManager_RemoveExpired(t *testing.T) {
	rm := NewRequestManager()
	ref := NewRef("test", "1")

	req := rm.Create(ref)

	// backdate SentAt so it appears expired
	rm.mu.Lock()
	req.SentAt = time.Now().Add(-10 * time.Second)
	rm.mu.Unlock()

	rm.RemoveExpired(5 * time.Second)

	// request should be removed from the map
	got := rm.Get(req.ID)
	if got != nil {
		t.Fatal("expected expired request to be removed")
	}

	// should have received a timeout response
	select {
	case res := <-req.Response:
		if res.Error != ErrRequestTimeout {
			t.Fatalf("expected ErrRequestTimeout, got %v", res.Error)
		}
	default:
		t.Fatal("expected timeout response on channel")
	}
}

func TestRequestManager_RemoveExpiredKeepsFresh(t *testing.T) {
	rm := NewRequestManager()
	ref := NewRef("test", "1")

	fresh := rm.Create(ref)

	expired := rm.Create(ref)
	rm.mu.Lock()
	expired.SentAt = time.Now().Add(-10 * time.Second)
	rm.mu.Unlock()

	rm.RemoveExpired(5 * time.Second)

	if rm.Get(fresh.ID) == nil {
		t.Fatal("fresh request should not have been removed")
	}
	if rm.Get(expired.ID) != nil {
		t.Fatal("expired request should have been removed")
	}
}

func TestRequestManager_PoolDrainsStaleResponses(t *testing.T) {
	rm := NewRequestManager()
	ref := NewRef("test", "1")

	// create a request and stuff a stale response into its channel
	req := rm.Create(ref)
	req.Response <- &Response{Body: "stale"}
	id := req.ID

	// return it to the pool
	rm.Remove(id)

	// create a new request — should get the pooled one with a drained channel
	req2 := rm.Create(ref)

	// channel should be empty
	select {
	case res := <-req2.Response:
		t.Fatalf("expected empty channel, got %v", res)
	default:
		// good — channel is empty
	}
}

func TestRequest_Timeout(t *testing.T) {
	req := &Request{
		ID:       1,
		Response: make(chan *Response, 1),
	}

	req.Timeout()

	select {
	case res := <-req.Response:
		if res.Error != ErrRequestTimeout {
			t.Fatalf("expected ErrRequestTimeout, got %v", res.Error)
		}
		if res.Body != nil {
			t.Fatalf("expected nil body, got %v", res.Body)
		}
	default:
		t.Fatal("expected response after Timeout()")
	}
}
