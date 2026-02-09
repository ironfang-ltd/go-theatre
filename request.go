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
	requests sync.Map // map[int64]*Request
	reqPool  sync.Pool
	reqID    int64
}

func NewRequestManager() *RequestManager {
	return &RequestManager{
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

	rm.requests.Store(r.ID, r)

	return r
}

func (rm *RequestManager) Get(id int64) *Request {
	v, ok := rm.requests.Load(id)
	if !ok {
		return nil
	}
	return v.(*Request)
}

func (rm *RequestManager) Remove(id int64) {
	v, loaded := rm.requests.LoadAndDelete(id)
	if !loaded {
		return
	}
	rm.reqPool.Put(v)
}

func (rm *RequestManager) RemoveExpired(requestTimeout time.Duration) int {
	expired := 0
	rm.requests.Range(func(key, value any) bool {
		req := value.(*Request)
		if time.Since(req.SentAt) > requestTimeout {
			rm.requests.Delete(key)
			req.Timeout()
			expired++
		}
		return true
	})
	return expired
}

// FailAll sends an error response to all pending requests and removes
// them from the manager. Used during freeze to unblock waiting callers.
func (rm *RequestManager) FailAll(err error) {
	rm.requests.Range(func(key, value any) bool {
		req := value.(*Request)
		req.Response <- &Response{Error: err}
		rm.requests.Delete(key)
		return true
	})
}
