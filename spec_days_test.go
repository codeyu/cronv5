package cron

import (
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestNextDayRules(t *testing.T) {
	tests := []struct {
		spec     string
		from     string
		expected []string // successive activations, starting after from
	}{
		// "L": last day of the month.
		{"0 0 L * *", "2026-01-15", []string{"2026-01-31", "2026-02-28", "2026-03-31"}},
		{"0 0 L 2 *", "2027-03-01", []string{"2028-02-29", "2029-02-28"}},
		{"0 0 l * *", "2026-01-15", []string{"2026-01-31"}},
		// "L-n": n days before the last day of the month.
		{"0 0 L-2 * *", "2026-02-01", []string{"2026-02-26", "2026-03-29"}},
		{"0 0 L-0 * *", "2026-02-01", []string{"2026-02-28"}},
		// "LW": last weekday of the month.
		{"0 0 LW * *", "2026-01-01", []string{"2026-01-30", "2026-02-27"}}, // Jan 31st is a Saturday
		{"0 0 LW 5 *", "2026-01-01", []string{"2026-05-29"}},               // May 31st is a Sunday
		{"0 0 LW 8 *", "2026-01-01", []string{"2026-08-31"}},               // Monday
		// "nW": weekday nearest to the n-th, without leaving the month.
		{"0 0 1W 8 *", "2026-01-01", []string{"2026-08-03"}},  // Saturday 1st -> Monday 3rd
		{"0 0 1W 2 *", "2026-01-01", []string{"2026-02-02"}},  // Sunday 1st -> Monday 2nd
		{"0 0 15W 2 *", "2026-01-01", []string{"2026-02-16"}}, // Sunday -> Monday
		{"0 0 15W 8 *", "2026-01-01", []string{"2026-08-14"}}, // Saturday -> Friday
		{"0 0 15W 1 *", "2026-01-01", []string{"2026-01-15"}}, // Thursday
		{"0 0 31W 5 *", "2026-01-01", []string{"2026-05-29"}}, // Sunday 31st -> Friday 29th
		{"0 0 31W * *", "2026-02-01", []string{"2026-03-31"}}, // February has no 31st
		// Lists mixing bits and day rules.
		{"0 0 15,L * *", "2026-02-01", []string{"2026-02-15", "2026-02-28", "2026-03-15"}},
		{"0 0 1,LW,L-5 * *", "2026-01-02", []string{"2026-01-26", "2026-01-30", "2026-02-01"}},

		// "d#k": k-th weekday d of the month.
		{"0 0 * * MON#1", "2026-01-01", []string{"2026-01-05", "2026-02-02"}},
		{"0 0 * * 1#1", "2026-01-01", []string{"2026-01-05"}},
		{"0 0 * * mon#1,fri#2", "2026-01-01", []string{"2026-01-05", "2026-01-09", "2026-02-02"}},
		{"0 0 * * 0#2", "2026-01-01", []string{"2026-01-11"}},
		{"0 0 * * 7#2", "2026-01-01", []string{"2026-01-11"}},
		{"0 0 * * FRI#5", "2026-01-01", []string{"2026-01-30", "2026-05-29"}},
		// "dL": last weekday d of the month.
		{"0 0 * * 5L", "2026-02-01", []string{"2026-02-27", "2026-03-27"}},
		{"0 0 * * FRIL", "2026-01-01", []string{"2026-01-30"}},
		{"0 0 * * SUNL", "2026-02-01", []string{"2026-02-22"}},
		{"0 0 * * 7L", "2026-02-01", []string{"2026-02-22"}},
		// Both day fields restricted: either one matches (Vixie cron semantics).
		{"0 0 L * MON#1", "2026-01-01", []string{"2026-01-05", "2026-01-31", "2026-02-02"}},
		// One of the day fields is a star: both must match.
		{"0 0 ? * MON#1", "2026-01-01", []string{"2026-01-05"}},

		// 7 is Sunday.
		{"0 0 * * 7", "2026-01-01", []string{"2026-01-04", "2026-01-11"}},
		{"0 0 * * 5-7", "2026-01-01", []string{"2026-01-02", "2026-01-03", "2026-01-04", "2026-01-09"}},
		{"0 0 * * 0-7", "2026-01-01", []string{"2026-01-02", "2026-01-03", "2026-01-04", "2026-01-05"}},
		{"0 0 * * 7-1", "2026-01-01", []string{"2026-01-04", "2026-01-05", "2026-01-11"}},
		// Wrap-around ranges.
		{"0 0 * * FRI-MON", "2026-01-06", []string{"2026-01-09", "2026-01-10", "2026-01-11", "2026-01-12", "2026-01-16"}},
		{"0 0 * * SAT-SUN", "2026-01-06", []string{"2026-01-10", "2026-01-11", "2026-01-17"}},
		{"0 0 * * 6-0", "2026-01-06", []string{"2026-01-10", "2026-01-11", "2026-01-17"}},
		{"0 0 * * FRI-TUE/2", "2026-01-06", []string{"2026-01-09", "2026-01-11", "2026-01-13", "2026-01-16"}},
		{"0 0 1 NOV-FEB *", "2026-03-01", []string{"2026-11-01", "2026-12-01", "2027-01-01", "2027-02-01", "2027-11-01"}},
	}
	for _, tt := range tests {
		t.Run(tt.spec, func(t *testing.T) {
			sched, err := ParseStandard("TZ=UTC " + tt.spec)
			if err != nil {
				t.Fatal(err)
			}
			next := mustDate(t, tt.from)
			for _, expected := range tt.expected {
				next = sched.Next(next)
				if got := next.Format(time.DateOnly); got != expected {
					t.Fatalf("expected %s, got %s", expected, got)
				}
			}
		})
	}
}

func TestDayRulesWithSeconds(t *testing.T) {
	sched, err := SecondParser.Parse("TZ=UTC 30 15 10 LW * *")
	if err != nil {
		t.Fatal(err)
	}
	got := sched.Next(mustDate(t, "2026-01-01"))
	if expected := time.Date(2026, 1, 30, 10, 15, 30, 0, time.UTC); !got.Equal(expected) {
		t.Fatalf("expected %v, got %v", expected, got)
	}
}

func TestUnsatisfiableDayRules(t *testing.T) {
	// These parse, but never activate (rejected by AddJob with ErrUnsatisfiableSchedule).
	for _, spec := range []string{"0 0 L-29 2 *", "0 0 L-30 2,4,6 *", "0 0 31W 2 *"} {
		sched, err := ParseStandard(spec)
		if err != nil {
			t.Fatalf("%s: %v", spec, err)
		}
		if next := sched.Next(time.Now()); !next.IsZero() {
			t.Errorf("%s: expected no activation, got %v", spec, next)
		}
		c := New().WithLogger(newNoOpLogger()).Build()
		if _, err := c.AddJob(spec, func() {}); !errors.Is(err, ErrUnsatisfiableSchedule) {
			t.Errorf("%s: expected ErrUnsatisfiableSchedule, got %v", spec, err)
		}
	}
}

func TestDayRulesErrors(t *testing.T) {
	tests := []struct {
		spec, err string
	}{
		{"0 0 L-31 * *", "above maximum (30)"},
		{"0 0 L-x * *", "failed to parse int"},
		{"0 0 L-1W * *", "failed to parse int"},
		{"0 0 0W * *", "out of range"},
		{"0 0 32W * *", "out of range"},
		{"0 0 W * *", "failed to parse int"},
		{"0 0 1-5W * *", "failed to parse int"},
		{"0 0 LL * *", "invalid use of L"},
		{"0 0 L/2 * *", "invalid use of L"},
		{"0 0 1-L * *", "invalid use of L"},
		{"0 0 * * MON#0", "out of range (1-5)"},
		{"0 0 * * MON#6", "out of range (1-5)"},
		{"0 0 * * MON#x", "failed to parse int"},
		{"0 0 * * MON#1-2", "failed to parse int"},
		{"0 0 * * #1", "failed to parse int"},
		{"0 0 * * 8#1", "above maximum"},
		{"0 0 * * L", "ambiguous"},
		{"0 0 * * 8L", "above maximum"},
		{"0 0 * * XL", "failed to parse int"},
		{"0 0 * * 8", "above maximum (7)"},
		{"0 0 * * 5-8", "above maximum (7)"},
		{"0 0 * * 6/2-1", "failed to parse int"},
		{"0 22-2 * * *", "beyond end of range"}, // only months and days of week wrap around
		{"0 0 L-2 * L", "ambiguous"},
	}
	for _, tt := range tests {
		_, err := ParseStandard(tt.spec)
		if err == nil || !strings.Contains(err.Error(), tt.err) {
			t.Errorf("%s: expected error containing %q, got %v", tt.spec, tt.err, err)
		}
	}
}

// TestDayRulesAgainstReference compares the activations of random day specs with a
// straightforward (slow) reference implementation, day by day over several years.
func TestDayRulesAgainstReference(t *testing.T) {
	domItems := []string{"*", "?", "1", "15", "31", "29", "1-7", "10-20/3", "*/5", "L", "L-1", "L-3", "L-27", "LW", "1W", "2W", "15W", "28W", "30W", "31W"}
	dowItems := []string{"*", "?", "0", "1", "5", "6", "7", "1-5", "5-7", "FRI-MON", "SAT-TUE", "6-1/2", "MON#1", "SUN#2", "fri#5", "wed#3", "7#1", "5L", "SUNL", "1L", "0L"}
	monthItems := []string{"*", "2", "NOV-FEB", "1-12/3", "OCT-MAR/2"}

	r := rand.New(rand.NewSource(1))
	pick := func(items []string) string {
		if r.Intn(3) == 0 {
			// List of 2 items; a star in a list is not meaningful.
			a, b := items[2+r.Intn(len(items)-2)], items[2+r.Intn(len(items)-2)]
			return a + "," + b
		}
		return items[r.Intn(len(items))]
	}

	start := time.Date(2025, 12, 25, 0, 0, 0, 0, time.UTC)
	end := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 300; i++ {
		domField, monthField, dowField := pick(domItems), pick(monthItems), pick(dowItems)
		spec := fmt.Sprintf("TZ=UTC 0 0 %s %s %s", domField, monthField, dowField)
		sched, err := ParseStandard(spec)
		if err != nil {
			t.Fatalf("%s: %v", spec, err)
		}
		// Expected activations, day by day.
		var expected []time.Time
		for d := start.AddDate(0, 0, 1); d.Before(end); d = d.AddDate(0, 0, 1) {
			if refDayMatches(t, d, domField, monthField, dowField) {
				expected = append(expected, d)
			}
		}
		next := start
		for _, e := range expected {
			prev := next
			next = sched.Next(prev)
			if !next.Equal(e) {
				t.Fatalf("%s: after %v, expected %v, got %v", spec, prev, e, next)
			}
		}
		if next = sched.Next(next); !next.IsZero() && next.Before(end) {
			t.Fatalf("%s: unexpected activation %v", spec, next)
		}
	}
}

func refDayMatches(t *testing.T, d time.Time, domField, monthField, dowField string) bool {
	last := time.Date(d.Year(), d.Month()+1, 0, 0, 0, 0, 0, time.UTC).Day()
	isWeekday := func(day int) bool {
		wd := time.Date(d.Year(), d.Month(), day, 0, 0, 0, 0, time.UTC).Weekday()
		return wd != time.Saturday && wd != time.Sunday
	}
	weekdays := map[string]int{"sun": 0, "mon": 1, "tue": 2, "wed": 3, "thu": 4, "fri": 5, "sat": 6}
	months := map[string]int{"jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6, "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12}
	num := func(s string, names map[string]int) int {
		if v, ok := names[strings.ToLower(s)]; ok {
			return v
		}
		v, err := strconv.Atoi(s)
		if err != nil {
			t.Fatalf("reference: bad number %q", s)
		}
		return v
	}
	// inRange reports whether v is in "a-b/step" (or "a", "*/step"), wrapping around [lo, hi].
	inRange := func(item string, v, lo, hi int, names map[string]int, sunday7 bool) bool {
		rangePart, stepPart, hasStep := strings.Cut(item, "/")
		step := 1
		if hasStep {
			step = num(stepPart, nil)
		}
		a, b := lo, hi
		if rangePart != "*" {
			from, to, isRange := strings.Cut(rangePart, "-")
			a = num(from, names)
			b = a
			if isRange {
				b = num(to, names)
			}
		}
		size := hi - lo + 1
		for k, x := 0, a; ; k, x = k+1, x+1 {
			if x > hi && !(sunday7 && x == 7 && b == 7) {
				x = lo
			}
			val := x
			if sunday7 && val == 7 {
				val = 0
			}
			if k%step == 0 && val == v {
				return true
			}
			if x == b || k > size+1 {
				return false
			}
		}
	}
	// nearest returns the weekday of the month closest to n.
	nearest := func(n int) int {
		best := -1
		for day := 1; day <= last; day++ {
			if isWeekday(day) && (best == -1 || abs(day-n) < abs(best-n)) {
				best = day
			}
		}
		return best
	}
	domMatch := func(item string) bool {
		day := d.Day()
		switch {
		case item == "*" || item == "?":
			return true
		case item == "L":
			return day == last
		case item == "LW":
			return day == nearest(last)
		case strings.HasPrefix(item, "L-"):
			return day == last-num(item[2:], nil)
		case strings.HasSuffix(item, "W"):
			n := num(item[:len(item)-1], nil)
			return n <= last && day == nearest(n)
		}
		return inRange(item, day, 1, 31, nil, false)
	}
	dowMatch := func(item string) bool {
		wd := int(d.Weekday())
		if before, after, ok := strings.Cut(item, "#"); ok {
			target := num(before, weekdays) % 7
			count := 0
			for day := 1; day <= d.Day(); day++ {
				if int(time.Date(d.Year(), d.Month(), day, 0, 0, 0, 0, time.UTC).Weekday()) == target {
					count++
				}
			}
			return wd == target && count == num(after, nil)
		}
		if strings.HasSuffix(item, "L") {
			target := num(item[:len(item)-1], weekdays) % 7
			lastSeen := 0
			for day := 1; day <= last; day++ {
				if int(time.Date(d.Year(), d.Month(), day, 0, 0, 0, 0, time.UTC).Weekday()) == target {
					lastSeen = day
				}
			}
			return d.Day() == lastSeen
		}
		if item == "*" || item == "?" {
			return true
		}
		return inRange(item, wd, 0, 6, weekdays, true)
	}
	any := func(field string, match func(string) bool) bool {
		for _, item := range strings.Split(field, ",") {
			if match(item) {
				return true
			}
		}
		return false
	}
	if !any(monthField, func(item string) bool { return item == "*" || inRange(item, int(d.Month()), 1, 12, months, false) }) {
		return false
	}
	domStar := domField == "*" || domField == "?"
	dowStar := dowField == "*" || dowField == "?"
	if domStar || dowStar {
		return any(domField, domMatch) && any(dowField, dowMatch)
	}
	return any(domField, domMatch) || any(dowField, dowMatch)
}

func abs(v int) int {
	if v < 0 {
		return -v
	}
	return v
}

func mustDate(t *testing.T, s string) time.Time {
	d, err := time.Parse(time.DateOnly, s)
	if err != nil {
		t.Fatal(err)
	}
	return d
}
