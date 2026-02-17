package theatre

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// ErrRequestTimeout is returned when a Request call exceeds the configured timeout.
var ErrRequestTimeout = fmt.Errorf("request timeout")

// Request tracks an in-flight request/response pair.
type Request struct {
	ID       int64
	To       Ref
	Response chan *Response
	sentAt   int64 // Unix seconds from coarse clock
}

// Timeout sends a timeout error response to the request's response channel.
func (r *Request) Timeout() {
	r.Response <- &Response{Error: ErrRequestTimeout}
}

const requestShards = 64

type requestShard struct {
	mu sync.RWMutex
	m  map[int64]*Request
}

type requestManager struct {
	shards  [requestShards]requestShard
	reqPool sync.Pool
	resPool *sync.Pool // shared Response pool (set by Host after construction)
	reqID   int64
}

func newRequestManager() *requestManager {
	rm := &requestManager{
		reqPool: sync.Pool{
			New: func() interface{} {
				return &Request{
					Response: make(chan *Response, 1024),
				}
			},
		},
	}
	for i := range rm.shards {
		rm.shards[i].m = make(map[int64]*Request)
	}
	return rm
}

func (rm *requestManager) shard(id int64) *requestShard {
	return &rm.shards[id&(requestShards-1)]
}

func (rm *requestManager) Create(ref Ref) *Request {

	reqID := atomic.AddInt64(&rm.reqID, 1)

	r := rm.reqPool.Get().(*Request)

	// drain any stale responses from a previous use
	for len(r.Response) > 0 {
		<-r.Response
	}

	r.ID = reqID
	r.To = ref
	r.sentAt = coarseNow.Load()

	s := rm.shard(reqID)
	s.mu.Lock()
	s.m[reqID] = r
	s.mu.Unlock()

	return r
}

func (rm *requestManager) Get(id int64) *Request {
	s := rm.shard(id)
	s.mu.RLock()
	r := s.m[id]
	s.mu.RUnlock()
	return r
}

func (rm *requestManager) Remove(id int64) {
	s := rm.shard(id)
	s.mu.Lock()
	r, ok := s.m[id]
	if ok {
		delete(s.m, id)
	}
	s.mu.Unlock()
	if ok {
		rm.reqPool.Put(r)
	}
}

func (rm *requestManager) RemoveExpired(requestTimeout time.Duration) int {
	expired := 0
	cutoff := coarseNow.Load() - int64(requestTimeout.Seconds())
	for i := range rm.shards {
		s := &rm.shards[i]
		s.mu.Lock()
		for id, req := range s.m {
			if req.sentAt < cutoff {
				delete(s.m, id)
				res := rm.getResponse()
				res.Error = ErrRequestTimeout
				req.Response <- res
				expired++
			}
		}
		s.mu.Unlock()
	}
	return expired
}

// FailAll sends an error response to all pending requests and removes
// them from the manager. Used during freeze to unblock waiting callers.
func (rm *requestManager) FailAll(err error) {
	for i := range rm.shards {
		s := &rm.shards[i]
		s.mu.Lock()
		for id, req := range s.m {
			res := rm.getResponse()
			res.Error = err
			req.Response <- res
			delete(s.m, id)
		}
		s.mu.Unlock()
	}
}

// getResponse returns a Response from the shared pool, or allocates
// a fresh one if the pool is not set (e.g. in standalone tests).
func (rm *requestManager) getResponse() *Response {
	if rm.resPool != nil {
		res := rm.resPool.Get().(*Response)
		res.Body = nil
		res.Error = nil
		return res
	}
	return &Response{}
}
