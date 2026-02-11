package theatre

import (
	"testing"
	"time"
)

func TestParseCron_Valid(t *testing.T) {
	tests := []struct {
		expr string
	}{
		{"* * * * *"},
		{"0 0 * * *"},
		{"*/15 * * * *"},
		{"0 2 * * 1-5"},
		{"30 4 1,15 * *"},
		{"0 0 1 1 *"},
		{"5-10 * * * *"},
		{"0 0 * * 0"},
		{"0 9-17 * * 1-5"},
		{"0 */2 * * *"},
	}
	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			cs, err := parseCron(tt.expr)
			if err != nil {
				t.Fatalf("parseCron(%q) error: %v", tt.expr, err)
			}
			if cs == nil {
				t.Fatal("parseCron returned nil schedule")
			}
		})
	}
}

func TestParseCron_Invalid(t *testing.T) {
	tests := []struct {
		expr string
	}{
		{""},
		{"* * *"},
		{"* * * * * *"},
		{"60 * * * *"},
		{"* 24 * * *"},
		{"* * 0 * *"},
		{"* * 32 * *"},
		{"* * * 0 *"},
		{"* * * 13 *"},
		{"* * * * 7"},
		{"abc * * * *"},
		{"*/0 * * * *"},
		{"5-3 * * * *"},
	}
	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			_, err := parseCron(tt.expr)
			if err == nil {
				t.Fatalf("parseCron(%q) expected error, got nil", tt.expr)
			}
		})
	}
}

func TestParseCron_FieldCounts(t *testing.T) {
	// "* * * * *" should set all bits true.
	cs, err := parseCron("* * * * *")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 60; i++ {
		if !cs.minute[i] {
			t.Errorf("minute[%d] should be true", i)
		}
	}
	for i := 0; i < 24; i++ {
		if !cs.hour[i] {
			t.Errorf("hour[%d] should be true", i)
		}
	}
	for i := 0; i < 31; i++ {
		if !cs.dayOfMonth[i] {
			t.Errorf("dayOfMonth[%d] should be true", i)
		}
	}
	for i := 0; i < 12; i++ {
		if !cs.month[i] {
			t.Errorf("month[%d] should be true", i)
		}
	}
	for i := 0; i < 7; i++ {
		if !cs.dayOfWeek[i] {
			t.Errorf("dayOfWeek[%d] should be true", i)
		}
	}
}

func TestParseCron_Step(t *testing.T) {
	cs, err := parseCron("*/15 * * * *")
	if err != nil {
		t.Fatal(err)
	}
	expected := map[int]bool{0: true, 15: true, 30: true, 45: true}
	for i := 0; i < 60; i++ {
		if cs.minute[i] != expected[i] {
			t.Errorf("minute[%d] = %v, want %v", i, cs.minute[i], expected[i])
		}
	}
}

func TestParseCron_RangeStep(t *testing.T) {
	cs, err := parseCron("1-10/3 * * * *")
	if err != nil {
		t.Fatal(err)
	}
	expected := map[int]bool{1: true, 4: true, 7: true, 10: true}
	for i := 0; i < 60; i++ {
		if cs.minute[i] != expected[i] {
			t.Errorf("minute[%d] = %v, want %v", i, cs.minute[i], expected[i])
		}
	}
}

func TestParseCron_CommaList(t *testing.T) {
	cs, err := parseCron("0 0 1,15,28 * *")
	if err != nil {
		t.Fatal(err)
	}
	expected := map[int]bool{0: true, 14: true, 27: true} // index 0 = day 1
	for i := 0; i < 31; i++ {
		if cs.dayOfMonth[i] != expected[i] {
			t.Errorf("dayOfMonth[%d] = %v, want %v", i, cs.dayOfMonth[i], expected[i])
		}
	}
}

func TestCronNext_EveryMinute(t *testing.T) {
	cs, _ := parseCron("* * * * *")
	from := time.Date(2025, 6, 15, 10, 30, 45, 0, time.UTC)
	next := cs.next(from)

	want := time.Date(2025, 6, 15, 10, 31, 0, 0, time.UTC)
	if !next.Equal(want) {
		t.Errorf("next = %v, want %v", next, want)
	}
}

func TestCronNext_TopOfHour(t *testing.T) {
	cs, _ := parseCron("0 * * * *")
	from := time.Date(2025, 6, 15, 10, 30, 0, 0, time.UTC)
	next := cs.next(from)

	want := time.Date(2025, 6, 15, 11, 0, 0, 0, time.UTC)
	if !next.Equal(want) {
		t.Errorf("next = %v, want %v", next, want)
	}
}

func TestCronNext_DailyAt2AM(t *testing.T) {
	cs, _ := parseCron("0 2 * * *")
	from := time.Date(2025, 6, 15, 3, 0, 0, 0, time.UTC)
	next := cs.next(from)

	want := time.Date(2025, 6, 16, 2, 0, 0, 0, time.UTC)
	if !next.Equal(want) {
		t.Errorf("next = %v, want %v", next, want)
	}
}

func TestCronNext_WeekdaysOnly(t *testing.T) {
	cs, _ := parseCron("0 9 * * 1-5")
	// Saturday June 14, 2025
	from := time.Date(2025, 6, 14, 10, 0, 0, 0, time.UTC)
	next := cs.next(from)

	// Monday June 16, 2025
	want := time.Date(2025, 6, 16, 9, 0, 0, 0, time.UTC)
	if !next.Equal(want) {
		t.Errorf("next = %v, want %v", next, want)
	}
}

func TestCronNext_Monthly(t *testing.T) {
	cs, _ := parseCron("0 0 1 * *")
	from := time.Date(2025, 6, 15, 0, 0, 0, 0, time.UTC)
	next := cs.next(from)

	want := time.Date(2025, 7, 1, 0, 0, 0, 0, time.UTC)
	if !next.Equal(want) {
		t.Errorf("next = %v, want %v", next, want)
	}
}

func TestCronNext_Yearly(t *testing.T) {
	cs, _ := parseCron("0 0 1 1 *")
	from := time.Date(2025, 2, 1, 0, 0, 0, 0, time.UTC)
	next := cs.next(from)

	want := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	if !next.Equal(want) {
		t.Errorf("next = %v, want %v", next, want)
	}
}

func TestCronNext_Every15Min(t *testing.T) {
	cs, _ := parseCron("*/15 * * * *")
	from := time.Date(2025, 6, 15, 10, 16, 0, 0, time.UTC)
	next := cs.next(from)

	want := time.Date(2025, 6, 15, 10, 30, 0, 0, time.UTC)
	if !next.Equal(want) {
		t.Errorf("next = %v, want %v", next, want)
	}
}

func TestCronNext_ExactMatch(t *testing.T) {
	// When "from" is exactly on a scheduled minute, next should be the NEXT one.
	cs, _ := parseCron("0 * * * *")
	from := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	next := cs.next(from)

	want := time.Date(2025, 6, 15, 11, 0, 0, 0, time.UTC)
	if !next.Equal(want) {
		t.Errorf("next = %v, want %v", next, want)
	}
}

func TestCronNext_CrossYear(t *testing.T) {
	cs, _ := parseCron("59 23 31 12 *")
	from := time.Date(2025, 12, 31, 23, 59, 0, 0, time.UTC)
	next := cs.next(from)

	want := time.Date(2026, 12, 31, 23, 59, 0, 0, time.UTC)
	if !next.Equal(want) {
		t.Errorf("next = %v, want %v", next, want)
	}
}

func TestCronNext_Feb29(t *testing.T) {
	// Schedule for the 29th — should find next leap year Feb 29.
	cs, _ := parseCron("0 0 29 2 *")
	from := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	next := cs.next(from)

	// 2028 is the next leap year.
	want := time.Date(2028, 2, 29, 0, 0, 0, 0, time.UTC)
	if !next.Equal(want) {
		t.Errorf("next = %v, want %v", next, want)
	}
}
