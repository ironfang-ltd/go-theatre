package theatre

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrRequestTimeout = fmt.Errorf("request timeout")
)

type Request struct {
	ID       int64
	To       Ref
	Response chan *Response
	SentAt   time.Time
}

func (r *Request) Timeout() {
	r.Response <- &Response{Error: ErrRequestTimeout}
}

type RequestManager struct {
	requests map[int64]*Request
	mu       sync.Mutex
	reqPool  sync.Pool
	reqID    int64
}

func NewRequestManager() *RequestManager {
	return &RequestManager{
		requests: make(map[int64]*Request),
		reqPool: sync.Pool{
			New: func() interface{} {
				return &Request{
					Response: make(chan *Response, 1024),
				}
			},
		},
	}
}

func (rm *RequestManager) Create(ref Ref) *Request {

	reqID := atomic.AddInt64(&rm.reqID, 1)

	r := rm.reqPool.Get().(*Request)

	// drain any stale responses from a previous use
	for len(r.Response) > 0 {
		<-r.Response
	}

	r.ID = reqID
	r.To = ref
	r.SentAt = time.Now()

	rm.mu.Lock()
	defer rm.mu.Unlock()

	rm.requests[r.ID] = r

	return r
}

func (rm *RequestManager) Get(id int64) *Request {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	return rm.requests[id]
}

func (rm *RequestManager) Remove(id int64) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	req := rm.requests[id]

	if req == nil {
		return
	}

	delete(rm.requests, id)

	rm.reqPool.Put(req)
}

func (rm *RequestManager) RemoveExpired() {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	for _, req := range rm.requests {
		if time.Since(req.SentAt) > 5*time.Second {
			delete(rm.requests, req.ID)
			req.Timeout()
		}
	}
}
