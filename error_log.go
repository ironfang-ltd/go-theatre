package theatre

import (
	"sync"
	"time"
)

// ErrorEntry represents a captured framework error or warning.
type ErrorEntry struct {
	Time    time.Time `json:"time"`
	Level   string    `json:"level"`            // "error" or "warn"
	Source  string    `json:"source"`            // "actor", "routing", "activation", "freeze", etc.
	Message string    `json:"message"`
	Actor   string    `json:"actor,omitempty"`   // "Type/ID" if actor-specific
	Detail  string    `json:"detail,omitempty"`  // error text
}

// ErrorLog is a thread-safe ring buffer of recent ErrorEntry values.
type ErrorLog struct {
	mu      sync.RWMutex
	entries []ErrorEntry
	pos     int // next write position
	count   int // total entries written (for wrap detection)
}

func newErrorLog(capacity int) *ErrorLog {
	return &ErrorLog{
		entries: make([]ErrorEntry, capacity),
	}
}

// Record adds an entry to the ring buffer, overwriting the oldest entry
// when capacity is reached.
func (el *ErrorLog) Record(e ErrorEntry) {
	el.mu.Lock()
	el.entries[el.pos] = e
	el.pos = (el.pos + 1) % len(el.entries)
	el.count++
	el.mu.Unlock()
}

// Recent returns up to n entries, newest first.
func (el *ErrorLog) Recent(n int) []ErrorEntry {
	el.mu.RLock()
	defer el.mu.RUnlock()

	cap := len(el.entries)
	total := el.count
	if total == 0 {
		return nil
	}

	// Number of valid entries in the buffer.
	valid := total
	if valid > cap {
		valid = cap
	}

	if n > valid {
		n = valid
	}

	result := make([]ErrorEntry, n)
	// pos-1 is the most recent entry (wrapped).
	for i := range n {
		idx := (el.pos - 1 - i + cap) % cap
		result[i] = el.entries[idx]
	}

	return result
}

// Total returns the total number of entries recorded (including overwritten).
func (el *ErrorLog) Total() int {
	el.mu.RLock()
	defer el.mu.RUnlock()
	return el.count
}
