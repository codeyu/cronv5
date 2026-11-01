package cron

import (
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"
	_ "time/tzdata" // the tests do not depend on the tz database of the machine
)

const dstLayout = "2006-01-02 15:04 -0700"

// Transitions in 2026 (wall clock just before -> just after):
//
//	America/New_York     03-08 02:00 -0500 -> 03:00 -0400   11-01 02:00 -0400 -> 01:00 -0500
//	Australia/Lord_Howe  10-04 02:00 +1030 -> 02:30 +1100   04-05 02:00 +1100 -> 01:30 +1030
//	America/Santiago     09-06 00:00 -0400 -> 01:00 -0300   04-05 00:00 -0300 -> 04-04 23:00 -0400
//	Pacific/Apia         2011-12-30 00:00 -1000 -> 2011-12-31 00:00 +1400 (December 30th 2011 skipped)
func TestNextDST(t *testing.T) {
	tests := []struct {
		zone, spec, from string
		expected         []string
	}{
		// Spring forward, the clock jumps from 2:00 to 3:00.
		// A time that does not exist runs at the end of the jump...
		{"America/New_York", "30 2 * * *", "2026-03-07 12:00 -0500", []string{"2026-03-08 03:00 -0400", "2026-03-09 02:30 -0400"}},
		{"America/New_York", "0 2 * * *", "2026-03-07 12:00 -0500", []string{"2026-03-08 03:00 -0400", "2026-03-09 02:00 -0400"}},
		// ... once, even if several times or the end of the jump itself match.
		{"America/New_York", "0,30 2,3 * * *", "2026-03-08 00:00 -0500", []string{"2026-03-08 03:00 -0400", "2026-03-08 03:30 -0400", "2026-03-09 02:00 -0400"}},
		// ... unless it is an interval schedule: it is skipped.
		{"America/New_York", "30 * * * *", "2026-03-08 00:00 -0500", []string{"2026-03-08 00:30 -0500", "2026-03-08 01:30 -0500", "2026-03-08 03:30 -0400"}},
		{"America/New_York", "*/20 * * * *", "2026-03-08 01:30 -0500", []string{"2026-03-08 01:40 -0500", "2026-03-08 03:00 -0400", "2026-03-08 03:20 -0400"}},
		{"America/New_York", "* 2 * * *", "2026-03-08 00:00 -0500", []string{"2026-03-09 02:00 -0400", "2026-03-09 02:01 -0400"}},
		{"America/New_York", "0 */2 * * *", "2026-03-08 00:00 -0500", []string{"2026-03-08 04:00 -0400"}},

		// Fall back, the clock goes back from 2:00 to 1:00.
		// A time that occurs twice runs the first time only...
		{"America/New_York", "30 1 * * *", "2026-10-31 12:00 -0400", []string{"2026-11-01 01:30 -0400", "2026-11-02 01:30 -0500"}},
		{"America/New_York", "0 1-3 * * *", "2026-11-01 00:00 -0400", []string{"2026-11-01 01:00 -0400", "2026-11-01 02:00 -0500", "2026-11-01 03:00 -0500"}},
		{"America/New_York", "30 1 * * *", "2026-11-01 01:10 -0500", []string{"2026-11-02 01:30 -0500"}},
		// ... unless it is an interval schedule: it runs both times.
		{"America/New_York", "0 * * * *", "2026-11-01 00:30 -0400", []string{"2026-11-01 01:00 -0400", "2026-11-01 01:00 -0500", "2026-11-01 02:00 -0500"}},
		{"America/New_York", "0 * * * *", "2026-11-01 01:10 -0400", []string{"2026-11-01 01:00 -0500"}},
		{"America/New_York", "30 * * * *", "2026-11-01 01:10 -0500", []string{"2026-11-01 01:30 -0500"}},
		{"America/New_York", "*/15 1 * * *", "2026-11-01 01:40 -0400", []string{
			"2026-11-01 01:45 -0400", "2026-11-01 01:00 -0500", "2026-11-01 01:15 -0500",
			"2026-11-01 01:30 -0500", "2026-11-01 01:45 -0500", "2026-11-02 01:00 -0500",
		}},

		// 30 minutes transitions.
		{"Australia/Lord_Howe", "15 2 * * *", "2026-10-03 12:00 +1030", []string{"2026-10-04 02:30 +1100", "2026-10-05 02:15 +1100"}},
		{"Australia/Lord_Howe", "*/15 * * * *", "2026-10-04 01:50 +1030", []string{"2026-10-04 02:30 +1100", "2026-10-04 02:45 +1100"}},
		{"Australia/Lord_Howe", "45 1 * * *", "2026-04-04 12:00 +1100", []string{"2026-04-05 01:45 +1100", "2026-04-06 01:45 +1030"}},
		{"Australia/Lord_Howe", "*/15 * * * *", "2026-04-05 01:40 +1100", []string{"2026-04-05 01:45 +1100", "2026-04-05 01:30 +1030", "2026-04-05 01:45 +1030", "2026-04-05 02:00 +1030"}},

		// Midnight does not exist / occurs twice.
		{"America/Santiago", "0 0 * * *", "2026-09-05 12:00 -0400", []string{"2026-09-06 01:00 -0300", "2026-09-07 00:00 -0300"}},
		{"America/Santiago", "30 23 * * *", "2026-04-04 12:00 -0300", []string{"2026-04-04 23:30 -0300", "2026-04-05 23:30 -0400"}},

		// A whole day skipped.
		{"Pacific/Apia", "0 12 * * *", "2011-12-29 13:00 -1000", []string{"2011-12-31 00:00 +1400", "2011-12-31 12:00 +1400"}},
		{"Pacific/Apia", "0 12 30 12 *", "2011-12-01 00:00 -1000", []string{"2011-12-31 00:00 +1400", "2012-12-30 12:00 +1400"}},
		{"Pacific/Apia", "0 * * * *", "2011-12-29 23:30 -1000", []string{"2011-12-31 00:00 +1400", "2011-12-31 01:00 +1400"}},

		// No DST.
		{"Asia/Tokyo", "30 2 * * *", "2026-03-07 12:00 +0900", []string{"2026-03-08 02:30 +0900", "2026-03-09 02:30 +0900"}},
	}
	for _, tt := range tests {
		t.Run(tt.zone+" "+tt.spec+" from "+tt.from, func(t *testing.T) {
			loc, err := time.LoadLocation(tt.zone)
			if err != nil {
				t.Fatal(err)
			}
			sched, err := ParseStandard("TZ=" + tt.zone + " " + tt.spec)
			if err != nil {
				t.Fatal(err)
			}
			next, err := time.Parse(dstLayout, tt.from)
			if err != nil {
				t.Fatal(err)
			}
			for _, expected := range tt.expected {
				next = sched.Next(next)
				if got := next.In(loc).Format(dstLayout); got != expected {
					t.Fatalf("expected %s, got %s", expected, got)
				}
			}
		})
	}
}

func TestNextDSTWithSeconds(t *testing.T) {
	sched, err := SecondParser.Parse("TZ=America/New_York 10 30 2 * * *")
	if err != nil {
		t.Fatal(err)
	}
	got := sched.Next(time.Date(2026, 3, 8, 0, 0, 0, 0, time.UTC))
	if expected := time.Date(2026, 3, 8, 7, 0, 0, 0, time.UTC); !got.Equal(expected) { // 3:00:00 EDT
		t.Fatalf("expected %v, got %v", expected, got)
	}
}

func TestNextDSTUnsatisfiable(t *testing.T) {
	// Every minute of 2am on the second Sunday of March: these times never exist in New York,
	// and are skipped by interval schedules.
	sched, err := ParseStandard("TZ=America/New_York * 2 * 3 SUN#2")
	if err != nil {
		t.Fatal(err)
	}
	if next := sched.Next(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)); !next.IsZero() {
		t.Fatalf("expected no activation, got %v", next)
	}
	// Not an interval: runs at 3am.
	sched, _ = ParseStandard("TZ=America/New_York 30 2 * 3 SUN#2")
	if next := sched.Next(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)); !next.Equal(time.Date(2026, 3, 8, 7, 0, 0, 0, time.UTC)) {
		t.Fatalf("expected 2026-03-08 03:00 EDT, got %v", next)
	}
}

// TestNextDSTAgainstReference compares the activations of random specs around DST transitions
// with a reference implementation walking the real time minute by minute.
func TestNextDSTAgainstReference(t *testing.T) {
	windows := []struct {
		zone string
		at   string // a transition, the window is the day before and the day after
	}{
		{"America/New_York", "2026-03-08 07:00 +0000"},
		{"America/New_York", "2026-11-01 06:00 +0000"},
		{"Europe/London", "2026-03-29 01:00 +0000"},
		{"Europe/London", "2026-10-25 01:00 +0000"},
		{"Australia/Lord_Howe", "2026-04-04 15:00 +0000"},
		{"Australia/Lord_Howe", "2026-10-03 15:30 +0000"},
		{"America/Santiago", "2026-04-05 03:00 +0000"},
		{"America/Santiago", "2026-09-06 04:00 +0000"},
		{"America/Havana", "2026-03-08 05:00 +0000"},
		{"America/Havana", "2026-11-01 05:00 +0000"},
		{"Pacific/Apia", "2011-12-30 10:00 +0000"},
		{"Asia/Tokyo", "2026-03-08 00:00 +0000"},
	}
	minuteItems := []string{"0", "30", "15", "45", "*", "*/15", "*/20", "0-59/7", "10-40/10", "5,35", "0,30"}
	hourItems := []string{"*", "*/2", "*/5", "0", "1", "2", "3", "23", "1-3", "0-3/2", "22-23", "0,1", "2,3"}
	dayItems := []string{"* * *", "* * *", "* * *", "L * *", "* * SUN", "30 * *", "31 12 *", "* * SAT"}

	r := rand.New(rand.NewSource(1))
	for _, w := range windows {
		loc, err := time.LoadLocation(w.zone)
		if err != nil {
			t.Fatal(err)
		}
		at, err := time.Parse(dstLayout, w.at)
		if err != nil {
			t.Fatal(err)
		}
		start, end := at.Add(-24*time.Hour), at.Add(48*time.Hour)
		for i := 0; i < 100; i++ {
			minute, hour := minuteItems[r.Intn(len(minuteItems))], hourItems[r.Intn(len(hourItems))]
			spec := fmt.Sprintf("%s %s %s", minute, hour, dayItems[r.Intn(len(dayItems))])
			sched, err := ParseStandard("TZ=" + w.zone + " " + spec)
			if err != nil {
				t.Fatal(err)
			}
			interval := strings.HasPrefix(minute, "*") || strings.HasPrefix(hour, "*")
			expected := refDSTActivations(sched.(*SpecSchedule), loc, start, end, interval)

			var got []string
			for next := sched.Next(start.Add(-time.Second)); !next.IsZero() && next.Before(end); next = sched.Next(next) {
				got = append(got, next.In(loc).Format(dstLayout))
			}
			if strings.Join(got, ", ") != strings.Join(expected, ", ") {
				t.Fatalf("%s %s around %s:\nexpected %v\ngot      %v", w.zone, spec, w.at, expected, got)
			}
		}
	}
}

// refDSTActivations walks the real time minute by minute between start and end, and returns the
// expected activations: an interval schedule runs whenever the clock shows a matching time, other
// schedules run the first time the clock shows a matching time, or when the clock jumps over one.
func refDSTActivations(s *SpecSchedule, loc *time.Location, start, end time.Time, interval bool) []string {
	wallOf := func(t time.Time) time.Time {
		t = t.In(loc)
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), 0, 0, time.UTC)
	}
	matches := func(wall time.Time) bool {
		return 1<<uint(wall.Minute())&s.Minute != 0 && 1<<uint(wall.Hour())&s.Hour != 0 &&
			1<<uint(wall.Month())&s.Month != 0 && dayMatches(s, wall)
	}
	var out []string
	seen := map[time.Time]bool{}
	prevWall := wallOf(start.Add(-time.Minute))
	for x := start; x.Before(end); x = x.Add(time.Minute) {
		wall := wallOf(x)
		run := false
		if !interval {
			for skipped := prevWall.Add(time.Minute); skipped.Before(wall); skipped = skipped.Add(time.Minute) {
				run = run || matches(skipped)
			}
		}
		if matches(wall) && (interval || !seen[wall]) {
			run = true
		}
		seen[wall] = true
		prevWall = wall
		if run {
			out = append(out, x.In(loc).Format(dstLayout))
		}
	}
	return out
}

// The scheduler runs a fixed time job once on DST transition days.
func TestCronDST(t *testing.T) {
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name, spec string
		start      time.Time
		expected   []string
	}{
		{"spring forward", "0 30 2 * * *", time.Date(2026, 3, 8, 5, 0, 0, 0, time.UTC), []string{"2026-03-08 03:00 -0400"}},
		{"fall back", "0 30 1 * * *", time.Date(2026, 11, 1, 4, 0, 0, 0, time.UTC), []string{"2026-11-01 01:30 -0400"}},
		{"fall back interval", "0 30 * * * *", time.Date(2026, 11, 1, 4, 0, 0, 0, time.UTC), []string{
			"2026-11-01 00:30 -0400", "2026-11-01 01:30 -0400", "2026-11-01 01:30 -0500", "2026-11-01 02:30 -0500",
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cron, clock := newRegressionCron(func(b *Builder) { b.WithLocation(ny) })
			clock.Advance(tt.start.Sub(clock.Now()))
			var (
				mu   sync.Mutex
				runs []string
			)
			_, err := cron.AddJob(tt.spec, func() {
				mu.Lock()
				defer mu.Unlock()
				runs = append(runs, clock.Now().In(ny).Format(dstLayout))
			})
			if err != nil {
				t.Fatal(err)
			}
			cron.Start()
			defer cron.Stop()
			cycle(cron)
			for i := 0; i < 4*60; i++ {
				advanceAndCycle(cron, time.Minute)
			}
			mu.Lock()
			defer mu.Unlock()
			if strings.Join(runs, ", ") != strings.Join(tt.expected, ", ") {
				t.Fatalf("expected %v, got %v", tt.expected, runs)
			}
		})
	}
}
