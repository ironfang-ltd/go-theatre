package theatre

import (
	"fmt"
	"sync"
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
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.len
}

func (r *RingBuffer[T]) Write(val T) error {

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.len == r.size {
		return ErrRingBufferFull
	}

	r.buf[r.writeIdx] = val
	r.writeIdx = (r.writeIdx + 1) % r.size

	r.len++

	return nil
}

func (r *RingBuffer[T]) Read() (T, bool) {

	var v T

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.len == 0 {
		return v, false
	}

	v = r.buf[r.readIdx]
	r.readIdx = (r.readIdx + 1) % r.size

	r.len--

	return v, true
}

func (r *RingBuffer[T]) ReadN(n int64) ([]T, bool) {

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.len == 0 {
		return nil, false
	}

	if n > r.len {
		n = r.len
	}

	var vals []T

	for i := int64(0); i < n; i++ {
		vals = append(vals, r.buf[(r.readIdx+i)%r.size])
	}

	r.readIdx = (r.readIdx + n) % r.size

	r.len -= n

	return vals, true
}
