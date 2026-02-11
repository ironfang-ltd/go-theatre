package theatre

import (
	"expvar"
	"strconv"
	"sync/atomic"
)

// metricsSeq generates unique IDs for expvar namespacing across hosts.
var metricsSeq atomic.Int64

// Metrics tracks operational counters for a Host. All counters are lock-free
// (atomic int64) and published to expvar under the "theatre." prefix for
// inspection via /debug/vars.
type Metrics struct {
	MessagesSent         atomic.Int64
	MessagesReceived     atomic.Int64
	MessagesDeadLettered atomic.Int64

	ActivationsTotal  atomic.Int64
	ActivationsFailed atomic.Int64

	RequestsTotal    atomic.Int64
	RequestsTimedOut atomic.Int64

	SchedulesFired     atomic.Int64
	SchedulesCancelled atomic.Int64
	SchedulesRecovered atomic.Int64

	FreezeCount atomic.Int64

	PlacementCacheHits   atomic.Int64
	PlacementCacheMisses atomic.Int64

	// actorCountFn returns the current number of active actors.
	// Set by Host at init time.
	actorCountFn func() int
}

// newMetrics creates a Metrics instance and publishes all counters to expvar.
// Each call gets a unique expvar prefix via a monotonic sequence.
func newMetrics() *Metrics {
	m := &Metrics{}

	// Use a monotonic sequence to guarantee unique expvar names even when
	// multiple hosts share the same hostRef (common in tests).
	seq := metricsSeq.Add(1)
	prefix := "theatre." + strconv.FormatInt(seq, 10) + "."

	publish := func(name string, v expvar.Var) {
		expvar.Publish(prefix+name, v)
	}

	publish("messages_sent", atomicVar(&m.MessagesSent))
	publish("messages_received", atomicVar(&m.MessagesReceived))
	publish("messages_dead_lettered", atomicVar(&m.MessagesDeadLettered))
	publish("activations_total", atomicVar(&m.ActivationsTotal))
	publish("activations_failed", atomicVar(&m.ActivationsFailed))
	publish("requests_total", atomicVar(&m.RequestsTotal))
	publish("requests_timed_out", atomicVar(&m.RequestsTimedOut))
	publish("schedules_fired", atomicVar(&m.SchedulesFired))
	publish("schedules_cancelled", atomicVar(&m.SchedulesCancelled))
	publish("schedules_recovered", atomicVar(&m.SchedulesRecovered))
	publish("freeze_count", atomicVar(&m.FreezeCount))
	publish("placement_cache_hits", atomicVar(&m.PlacementCacheHits))
	publish("placement_cache_misses", atomicVar(&m.PlacementCacheMisses))
	publish("actors_active", expvar.Func(func() any {
		if m.actorCountFn != nil {
			return m.actorCountFn()
		}
		return 0
	}))

	return m
}

// atomicVar wraps an *atomic.Int64 as an expvar.Var.
func atomicVar(v *atomic.Int64) expvar.Var {
	return expvar.Func(func() any {
		return v.Load()
	})
}

// Snapshot returns all metric values as a map, suitable for JSON serialization.
func (m *Metrics) Snapshot() map[string]int64 {
	snap := map[string]int64{
		"messages_sent":          m.MessagesSent.Load(),
		"messages_received":      m.MessagesReceived.Load(),
		"messages_dead_lettered": m.MessagesDeadLettered.Load(),
		"activations_total":      m.ActivationsTotal.Load(),
		"activations_failed":     m.ActivationsFailed.Load(),
		"requests_total":         m.RequestsTotal.Load(),
		"requests_timed_out":     m.RequestsTimedOut.Load(),
		"schedules_fired":       m.SchedulesFired.Load(),
		"schedules_cancelled":   m.SchedulesCancelled.Load(),
		"schedules_recovered":   m.SchedulesRecovered.Load(),
		"freeze_count":           m.FreezeCount.Load(),
		"placement_cache_hits":   m.PlacementCacheHits.Load(),
		"placement_cache_misses": m.PlacementCacheMisses.Load(),
	}
	if m.actorCountFn != nil {
		snap["actors_active"] = int64(m.actorCountFn())
	}
	return snap
}

