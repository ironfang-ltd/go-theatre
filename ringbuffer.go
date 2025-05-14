package theater

import (
	"fmt"
	"sync"
	"sync/atomic"
)

var (
	ErrRingBufferFull = fmt.Errorf("ring buffer is full")
)

type RingBuffer[T any] struct {
	len      int64
	buf      []T
	readIdx  int64
	writeIdx int64
	size     int64
	mu       sync.Mutex
}

func NewRingBuffer[T any](size int64) *RingBuffer[T] {
	return &RingBuffer[T]{
		len:      0,
		buf:      make([]T, size),
		readIdx:  0,
		writeIdx: 0,
		size:     size,
		mu:       sync.Mutex{},
	}
}

func (r *RingBuffer[T]) Len() int64 {
	return atomic.LoadInt64(&r.len)
}

func (r *RingBuffer[T]) Write(val T) error {

	if r.Len() == r.size {
		return ErrRingBufferFull
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.buf[r.writeIdx] = val
	r.writeIdx = (r.writeIdx + 1) % r.size

	atomic.AddInt64(&r.len, 1)

	return nil
}

func (r *RingBuffer[T]) Read() (T, bool) {

	var v T

	if r.Len() == 0 {
		return v, false
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	v = r.buf[r.readIdx]
	r.readIdx = (r.readIdx + 1) % r.size

	atomic.AddInt64(&r.len, -1)

	return v, true
}

func (r *RingBuffer[T]) ReadN(n int64) ([]T, bool) {

	if r.Len() == 0 {
		return nil, false
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if n > r.Len() {
		n = r.Len()
	}

	var vals []T

	for i := int64(0); i < n; i++ {
		vals = append(vals, r.buf[(r.readIdx+i)%r.size])
	}

	r.readIdx = (r.readIdx + n) % r.size

	atomic.AddInt64(&r.len, -n)

	return vals, true
}
