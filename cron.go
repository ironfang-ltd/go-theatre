package theatre

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// cronSchedule represents a parsed 5-field cron expression.
// Fields: minute(0-59) hour(0-23) day-of-month(1-31) month(1-12) day-of-week(0-6, 0=Sun).
type cronSchedule struct {
	minute     [60]bool
	hour       [24]bool
	dayOfMonth [31]bool // index 0 = day 1
	month      [12]bool // index 0 = January
	dayOfWeek  [7]bool  // index 0 = Sunday
}

// parseCron parses a standard 5-field cron expression.
// Supported syntax per field: *, N, N-M, */S, N-M/S, comma-separated lists.
func parseCron(expr string) (*cronSchedule, error) {
	fields := strings.Fields(expr)
	if len(fields) != 5 {
		return nil, fmt.Errorf("cron: expected 5 fields, got %d", len(fields))
	}

	cs := &cronSchedule{}

	if err := parseField(fields[0], cs.minute[:], 0, 59); err != nil {
		return nil, fmt.Errorf("cron: minute: %w", err)
	}
	if err := parseField(fields[1], cs.hour[:], 0, 23); err != nil {
		return nil, fmt.Errorf("cron: hour: %w", err)
	}
	if err := parseField(fields[2], cs.dayOfMonth[:], 1, 31); err != nil {
		return nil, fmt.Errorf("cron: day-of-month: %w", err)
	}
	if err := parseField(fields[3], cs.month[:], 1, 12); err != nil {
		return nil, fmt.Errorf("cron: month: %w", err)
	}
	if err := parseField(fields[4], cs.dayOfWeek[:], 0, 6); err != nil {
		return nil, fmt.Errorf("cron: day-of-week: %w", err)
	}

	return cs, nil
}

// parseField parses a single cron field into a boolean slice.
// min/max define the valid range for values. The slice is indexed from 0,
// so for fields where min > 0 (day-of-month, month), values are offset by min.
func parseField(field string, bits []bool, min, max int) error {
	for _, part := range strings.Split(field, ",") {
		if err := parsePart(part, bits, min, max); err != nil {
			return err
		}
	}
	return nil
}

func parsePart(part string, bits []bool, min, max int) error {
	// Split on "/" for step values.
	step := 1
	if idx := strings.Index(part, "/"); idx != -1 {
		s, err := strconv.Atoi(part[idx+1:])
		if err != nil || s <= 0 {
			return fmt.Errorf("invalid step %q", part[idx+1:])
		}
		step = s
		part = part[:idx]
	}

	var lo, hi int

	switch {
	case part == "*":
		lo, hi = min, max
	case strings.Contains(part, "-"):
		rangeParts := strings.SplitN(part, "-", 2)
		var err error
		lo, err = strconv.Atoi(rangeParts[0])
		if err != nil {
			return fmt.Errorf("invalid range start %q", rangeParts[0])
		}
		hi, err = strconv.Atoi(rangeParts[1])
		if err != nil {
			return fmt.Errorf("invalid range end %q", rangeParts[1])
		}
	default:
		v, err := strconv.Atoi(part)
		if err != nil {
			return fmt.Errorf("invalid value %q", part)
		}
		lo, hi = v, v
	}

	if lo < min || hi > max || lo > hi {
		return fmt.Errorf("range %d-%d out of bounds [%d,%d]", lo, hi, min, max)
	}

	for v := lo; v <= hi; v += step {
		bits[v-min] = true
	}
	return nil
}

// next returns the next fire time strictly after from. Returns zero time if
// no valid time is found within 4 years (safety cap).
func (cs *cronSchedule) next(from time.Time) time.Time {
	// Truncate to minute, then advance by 1 minute.
	t := from.Truncate(time.Minute).Add(time.Minute)

	// Safety cap: don't search more than 4 years.
	end := from.Add(4 * 365 * 24 * time.Hour)

	for t.Before(end) {
		// Check month (1-12, index 0 = January).
		if !cs.month[t.Month()-1] {
			// Advance to the first day of next month.
			t = time.Date(t.Year(), t.Month()+1, 1, 0, 0, 0, 0, t.Location())
			continue
		}

		// Check day-of-month (1-31, index 0 = day 1).
		if !cs.dayOfMonth[t.Day()-1] {
			t = time.Date(t.Year(), t.Month(), t.Day()+1, 0, 0, 0, 0, t.Location())
			continue
		}

		// Check day-of-week (0-6, 0 = Sunday).
		if !cs.dayOfWeek[t.Weekday()] {
			t = time.Date(t.Year(), t.Month(), t.Day()+1, 0, 0, 0, 0, t.Location())
			continue
		}

		// Check hour (0-23).
		if !cs.hour[t.Hour()] {
			t = time.Date(t.Year(), t.Month(), t.Day(), t.Hour()+1, 0, 0, 0, t.Location())
			continue
		}

		// Check minute (0-59).
		if !cs.minute[t.Minute()] {
			t = t.Add(time.Minute)
			continue
		}

		return t
	}

	return time.Time{}
}
