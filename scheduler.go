package theatre

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/gob"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// ScheduleID uniquely identifies a scheduled message.
type ScheduleID int64

type schedule struct {
	id       ScheduleID
	ref      Ref
	body     interface{}
	cron     *cronSchedule // nil for one-shot
	cronExpr string        // raw expression for DB persistence
	nextFire time.Time
	oneShot  bool
	persisted bool // true if this schedule exists in DB
}

// Scheduler manages delayed and recurring message delivery for a Host.
// It uses a single timer that sleeps until the earliest nextFire time.
type Scheduler struct {
	host      *Host
	mu        sync.Mutex
	schedules map[ScheduleID]*schedule
	nextID    atomic.Int64
	timer     *time.Timer
	notify    chan struct{} // buffered(1), poked on add/cancel
	done      chan struct{}

	// Cluster persistence (nil in standalone mode).
	db          SQLDB
	localHostID string
}

func newScheduler(host *Host) *Scheduler {
	return &Scheduler{
		host:      host,
		schedules: make(map[ScheduleID]*schedule),
		notify:    make(chan struct{}, 1),
		done:      make(chan struct{}),
	}
}

// setDB wires the database for persistent schedules. Called from SetCluster.
func (s *Scheduler) setDB(db SQLDB, hostID string) {
	s.db = db
	s.localHostID = hostID
}

// run is the scheduler's main goroutine. It sleeps until the earliest
// nextFire time, then fires all due schedules.
func (s *Scheduler) run() {
	s.timer = time.NewTimer(time.Hour) // initial long sleep
	s.timer.Stop()

	for {
		dur := s.timeUntilNext()
		if dur > 0 {
			s.timer.Reset(dur)
		} else {
			// No schedules — park until notified.
			s.timer.Reset(time.Hour)
		}

		select {
		case <-s.done:
			s.timer.Stop()
			return
		case <-s.notify:
			s.timer.Stop()
			// Drain timer channel if it fired between stop and select.
			select {
			case <-s.timer.C:
			default:
			}
			// Re-loop to recalculate the next fire time.
		case <-s.timer.C:
			s.fireDue()
		}
	}
}

// recoveryLoop polls the database for overdue schedules and fires them.
// Only runs in cluster mode when a DB is available.
func (s *Scheduler) recoveryLoop() {
	ticker := time.NewTicker(s.host.config.scheduleRecoveryInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.done:
			return
		case <-ticker.C:
			s.recoverOverdue()
		}
	}
}

// stop signals the scheduler goroutine to exit.
func (s *Scheduler) stop() {
	close(s.done)
}

// add inserts a new schedule and wakes the run loop.
func (s *Scheduler) add(ref Ref, body interface{}, cron *cronSchedule, cronExpr string, nextFire time.Time, oneShot bool) ScheduleID {
	var id ScheduleID
	persisted := false

	if s.db != nil {
		encoded, err := encodeScheduleBody(body)
		if err != nil {
			slog.Error("scheduler: failed to encode body for persistence", "error", err)
		} else {
			var dbID int64
			err = s.db.QueryRowContext(context.Background(), `
				INSERT INTO schedules (actor_type, actor_id, body, cron_expr, next_fire, one_shot, created_by)
				VALUES ($1, $2, $3, $4, $5, $6, $7)
				RETURNING schedule_id
			`, ref.Type, ref.ID, encoded, cronExpr, nextFire.Truncate(time.Microsecond), oneShot, s.localHostID).Scan(&dbID)
			if err != nil {
				slog.Error("scheduler: failed to persist schedule, falling back to local", "error", err)
			} else {
				id = ScheduleID(dbID)
				persisted = true
			}
		}
	}

	if !persisted {
		id = ScheduleID(s.nextID.Add(1))
	}

	s.mu.Lock()
	s.schedules[id] = &schedule{
		id:        id,
		ref:       ref,
		body:      body,
		cron:      cron,
		cronExpr:  cronExpr,
		nextFire:  nextFire,
		oneShot:   oneShot,
		persisted: persisted,
	}
	s.mu.Unlock()

	// Poke the run loop to recalculate the next fire time.
	select {
	case s.notify <- struct{}{}:
	default:
	}

	return id
}

// cancel removes a schedule by ID. Returns an error if the DB delete fails.
// Best-effort: if the message is already copied for firing, it still fires.
func (s *Scheduler) cancel(id ScheduleID) error {
	s.mu.Lock()
	_, ok := s.schedules[id]
	if ok {
		delete(s.schedules, id)
	}
	s.mu.Unlock()

	if ok {
		s.host.metrics.SchedulesCancelled.Add(1)
		// Poke the run loop.
		select {
		case s.notify <- struct{}{}:
		default:
		}
	}

	// Delete from DB if cluster mode is active.
	if s.db != nil {
		_, err := s.db.ExecContext(context.Background(),
			`DELETE FROM schedules WHERE schedule_id = $1`, int64(id))
		if err != nil {
			return err
		}
	}

	return nil
}

// cancelAll removes all schedules from the local map. Called during Freeze.
// Does NOT delete DB rows — other hosts can pick them up via recovery.
func (s *Scheduler) cancelAll() {
	s.mu.Lock()
	n := int64(len(s.schedules))
	s.schedules = make(map[ScheduleID]*schedule)
	s.mu.Unlock()

	if n > 0 {
		s.host.metrics.SchedulesCancelled.Add(n)
		select {
		case s.notify <- struct{}{}:
		default:
		}
	}
}

// count returns the number of pending schedules.
func (s *Scheduler) count() int {
	s.mu.Lock()
	n := len(s.schedules)
	s.mu.Unlock()
	return n
}

// timeUntilNext returns the duration until the earliest nextFire,
// or 0 if there are no schedules.
func (s *Scheduler) timeUntilNext() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.schedules) == 0 {
		return 0
	}

	var earliest time.Time
	for _, sched := range s.schedules {
		if earliest.IsZero() || sched.nextFire.Before(earliest) {
			earliest = sched.nextFire
		}
	}

	dur := time.Until(earliest)
	if dur < 0 {
		dur = 0
	}
	return dur
}

// fireDue collects all schedules where nextFire <= now, attempts atomic
// DB claims for persisted schedules, and fires messages outside the lock.
func (s *Scheduler) fireDue() {
	now := time.Now()

	type pending struct {
		id        ScheduleID
		ref       Ref
		body      interface{}
		cron      *cronSchedule
		cronExpr  string
		persisted bool
		oneShot   bool
	}

	var toFire []pending

	s.mu.Lock()
	for id, sched := range s.schedules {
		if !sched.nextFire.After(now) {
			toFire = append(toFire, pending{
				id:        id,
				ref:       sched.ref,
				body:      sched.body,
				cron:      sched.cron,
				cronExpr:  sched.cronExpr,
				persisted: sched.persisted,
				oneShot:   sched.oneShot,
			})

			if sched.persisted {
				// Remove from local map pending DB claim verification.
				// Will re-add for cron if claim succeeds.
				delete(s.schedules, id)
			} else if sched.oneShot {
				delete(s.schedules, id)
			} else {
				// Non-persisted cron: recompute next fire time.
				sched.nextFire = sched.cron.next(now)
				if sched.nextFire.IsZero() {
					// Impossible schedule — remove it.
					delete(s.schedules, id)
				}
			}
		}
	}
	s.mu.Unlock()

	// Fire outside the lock via host.Send. This respects drain/freeze
	// flags, auto-creates actors, and routes through cluster.
	for _, p := range toFire {
		if p.persisted {
			if !s.claimAndMaybeReschedule(p.id, p.ref, p.body, p.cron, p.cronExpr, p.oneShot, now) {
				continue // another host handled it
			}
		}
		s.host.Send(p.ref, p.body)
		s.host.metrics.SchedulesFired.Add(1)
	}
}

// claimAndMaybeReschedule performs an atomic DB claim for a persisted schedule.
// Returns true if this host won the claim and should fire the message.
// For cron schedules that win the claim, re-adds to the local map.
func (s *Scheduler) claimAndMaybeReschedule(id ScheduleID, ref Ref, body interface{}, cron *cronSchedule, cronExpr string, oneShot bool, now time.Time) bool {
	if oneShot {
		result, err := s.db.ExecContext(context.Background(),
			`DELETE FROM schedules WHERE schedule_id = $1 AND next_fire <= now()`, int64(id))
		if err != nil {
			slog.Error("scheduler: one-shot claim failed", "id", id, "error", err)
			return false
		}
		affected, _ := result.RowsAffected()
		return affected > 0
	}

	// Cron: compute next fire time and atomically advance.
	nextFire := cron.next(now)
	if nextFire.IsZero() {
		// Can't compute next fire — delete from DB.
		s.db.ExecContext(context.Background(),
			`DELETE FROM schedules WHERE schedule_id = $1`, int64(id))
		return false
	}

	result, err := s.db.ExecContext(context.Background(),
		`UPDATE schedules SET next_fire = $1 WHERE schedule_id = $2 AND next_fire <= now()`,
		nextFire.Truncate(time.Microsecond), int64(id))
	if err != nil {
		slog.Error("scheduler: cron claim failed", "id", id, "error", err)
		return false
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return false // another host handled it
	}

	// Re-add to local map for continued tracking.
	s.mu.Lock()
	s.schedules[id] = &schedule{
		id:        id,
		ref:       ref,
		body:      body,
		cron:      cron,
		cronExpr:  cronExpr,
		nextFire:  nextFire,
		persisted: true,
	}
	s.mu.Unlock()

	// Poke run loop to pick up the new next fire time.
	select {
	case s.notify <- struct{}{}:
	default:
	}

	return true
}

// recoverOverdue queries the DB for overdue schedules and claims them.
// Called periodically by recoveryLoop to handle schedules whose host crashed.
func (s *Scheduler) recoverOverdue() {
	rows, err := s.db.QueryContext(context.Background(), `
		SELECT schedule_id, actor_type, actor_id, body, cron_expr, next_fire, one_shot
		FROM schedules
		WHERE next_fire <= now()
		ORDER BY next_fire
		LIMIT 100
	`)
	if err != nil {
		slog.Error("scheduler: recovery query failed", "error", err)
		return
	}
	defer rows.Close()

	now := time.Now()
	for rows.Next() {
		var (
			dbID      int64
			actorType string
			actorID   string
			bodyData  []byte
			cronExpr  sql.NullString
			nextFire  time.Time
			oneShot   bool
		)
		if err := rows.Scan(&dbID, &actorType, &actorID, &bodyData, &cronExpr, &nextFire, &oneShot); err != nil {
			slog.Error("scheduler: recovery scan failed", "error", err)
			continue
		}

		id := ScheduleID(dbID)

		// Skip if already in local map (our own timer will handle it).
		s.mu.Lock()
		_, exists := s.schedules[id]
		s.mu.Unlock()
		if exists {
			continue
		}

		body, err := decodeScheduleBody(bodyData)
		if err != nil {
			slog.Error("scheduler: recovery decode failed", "scheduleID", dbID, "error", err)
			continue
		}

		ref := NewRef(actorType, actorID)

		if oneShot {
			// Atomic DELETE claim.
			result, err := s.db.ExecContext(context.Background(),
				`DELETE FROM schedules WHERE schedule_id = $1 AND next_fire <= now()`, dbID)
			if err != nil {
				slog.Error("scheduler: recovery one-shot claim failed", "scheduleID", dbID, "error", err)
				continue
			}
			affected, _ := result.RowsAffected()
			if affected == 0 {
				continue // another host claimed it
			}
			s.host.Send(ref, body)
			s.host.metrics.SchedulesFired.Add(1)
			s.host.metrics.SchedulesRecovered.Add(1)
		} else {
			// Cron: parse expression, compute next fire, claim.
			expr := ""
			if cronExpr.Valid {
				expr = cronExpr.String
			}
			if expr == "" {
				// No cron expression — can't reschedule. Delete the stale row.
				s.db.ExecContext(context.Background(),
					`DELETE FROM schedules WHERE schedule_id = $1`, dbID)
				continue
			}

			cs, err := parseCron(expr)
			if err != nil {
				slog.Error("scheduler: recovery cron parse failed", "expr", expr, "error", err)
				continue
			}

			newNextFire := cs.next(now)
			if newNextFire.IsZero() {
				s.db.ExecContext(context.Background(),
					`DELETE FROM schedules WHERE schedule_id = $1`, dbID)
				continue
			}

			// Atomic UPDATE claim.
			result, err := s.db.ExecContext(context.Background(),
				`UPDATE schedules SET next_fire = $1 WHERE schedule_id = $2 AND next_fire <= now()`,
				newNextFire.Truncate(time.Microsecond), dbID)
			if err != nil {
				slog.Error("scheduler: recovery cron claim failed", "scheduleID", dbID, "error", err)
				continue
			}
			affected, _ := result.RowsAffected()
			if affected == 0 {
				continue
			}

			// Fire once, then add to local map for continued tracking.
			s.host.Send(ref, body)
			s.host.metrics.SchedulesFired.Add(1)
			s.host.metrics.SchedulesRecovered.Add(1)

			s.mu.Lock()
			s.schedules[id] = &schedule{
				id:        id,
				ref:       ref,
				body:      body,
				cron:      cs,
				cronExpr:  expr,
				nextFire:  newNextFire,
				persisted: true,
			}
			s.mu.Unlock()

			// Poke run loop to pick up the new schedule.
			select {
			case s.notify <- struct{}{}:
			default:
			}
		}
	}
}

// --- body encoding ---

func encodeScheduleBody(body interface{}) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&body); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeScheduleBody(data []byte) (interface{}, error) {
	var body interface{}
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&body); err != nil {
		return nil, err
	}
	return body, nil
}

// --- Host-level schedule API ---

// SendAfter schedules a one-shot message to be sent after the given delay.
func (m *Host) SendAfter(ref Ref, body interface{}, delay time.Duration) (ScheduleID, error) {
	if delay <= 0 {
		return 0, fmt.Errorf("scheduler: delay must be positive")
	}
	id := m.scheduler.add(ref, body, nil, "", time.Now().Add(delay), true)
	return id, nil
}

// SendCron schedules a recurring message using a 5-field cron expression.
// Format: "minute hour day-of-month month day-of-week".
func (m *Host) SendCron(ref Ref, body interface{}, cronExpr string) (ScheduleID, error) {
	cs, err := parseCron(cronExpr)
	if err != nil {
		return 0, err
	}

	nextFire := cs.next(time.Now())
	if nextFire.IsZero() {
		return 0, fmt.Errorf("scheduler: cron expression %q has no valid fire time", cronExpr)
	}

	id := m.scheduler.add(ref, body, cs, cronExpr, nextFire, false)
	return id, nil
}

// CancelSchedule removes a scheduled message. Returns an error if the DB
// delete fails (cluster mode). In standalone mode, always returns nil.
func (m *Host) CancelSchedule(id ScheduleID) error {
	return m.scheduler.cancel(id)
}
