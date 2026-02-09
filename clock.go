package theatre

import (
	"sync/atomic"
	"time"
)

// coarseNow is a cached Unix timestamp updated every 500ms by a background
// goroutine. Used in place of time.Now().Unix() on ultra-hot paths (e.g.
// actor lastMessage tracking) to avoid a syscall per message.
var coarseNow atomic.Int64

func init() {
	coarseNow.Store(time.Now().Unix())
	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		for range ticker.C {
			coarseNow.Store(time.Now().Unix())
		}
	}()
}
