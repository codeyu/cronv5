package cron

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPrev(t *testing.T) {
	tests := []struct {
		zone, spec, at, expected string
	}{
		{"UTC", "0 0 L * *", "2026-03-15 00:00 +0000", "2026-02-28 00:00 +0000"},
		{"UTC", "0 0 L * *", "2026-02-28 00:00 +0000", "2026-01-31 00:00 +0000"}, // strictly before
		{"UTC", "0 0 * * MON#1", "2026-01-01 00:00 +0000", "2025-12-01 00:00 +0000"},
		{"UTC", "0 0 1 NOV-FEB *", "2026-07-01 00:00 +0000", "2026-02-01 00:00 +0000"},
		{"UTC", "*/15 * * * *", "2026-01-01 00:00 +0000", "2025-12-31 23:45 +0000"},
		{"UTC", "0 0 29 2 *", "2027-01-01 00:00 +0000", "2024-02-29 00:00 +0000"},
		{"Asia/Tokyo", "30 2 * * *", "2026-03-08 02:00 +0900", "2026-03-07 02:30 +0900"},
		// Spring forward: a fixed time runs at the end of the jump.
		{"America/New_York", "30 2 * * *", "2026-03-08 04:00 -0400", "2026-03-08 03:00 -0400"},
		{"America/New_York", "30 2 * * *", "2026-03-08 03:00 -0400", "2026-03-07 02:30 -0500"}, // strictly before
		{"America/New_York", "30 2 * * *", "2026-03-08 03:01 -0400", "2026-03-08 03:00 -0400"},
		{"America/New_York", "30 * * * *", "2026-03-08 03:20 -0400", "2026-03-08 01:30 -0500"},
		// Fall back: a fixed time runs the first time only, an interval both times.
		{"America/New_York", "30 1 * * *", "2026-11-01 01:45 -0500", "2026-11-01 01:30 -0400"},
		{"America/New_York", "30 1 * * *", "2026-11-01 01:10 -0500", "2026-11-01 01:30 -0400"},
		{"America/New_York", "0 * * * *", "2026-11-01 01:30 -0500", "2026-11-01 01:00 -0500"},
		{"America/New_York", "0 * * * *", "2026-11-01 01:00 -0500", "2026-11-01 01:00 -0400"},
		{"America/New_York", "45 * * * *", "2026-11-01 01:10 -0500", "2026-11-01 01:45 -0400"},
		// A whole day skipped.
		{"Pacific/Apia", "0 12 * * *", "2011-12-31 06:00 +1400", "2011-12-31 00:00 +1400"},
	}
	for _, tt := range tests {
		t.Run(tt.zone+" "+tt.spec+" before "+tt.at, func(t *testing.T) {
			loc, err := time.LoadLocation(tt.zone)
			if err != nil {
				t.Fatal(err)
			}
			sched, err := ParseStandard("TZ=" + tt.zone + " " + tt.spec)
			if err != nil {
				t.Fatal(err)
			}
			at, err := time.Parse(dstLayout, tt.at)
			if err != nil {
				t.Fatal(err)
			}
			if got := sched.(*SpecSchedule).Prev(at).In(loc).Format(dstLayout); got != tt.expected {
				t.Fatalf("expected %s, got %s", tt.expected, got)
			}
		})
	}
}

func TestPrevWithSecondsAndNanoseconds(t *testing.T) {
	sched, err := SecondParser.Parse("TZ=UTC */10 * * * * *")
	if err != nil {
		t.Fatal(err)
	}
	s := sched.(*SpecSchedule)
	at := time.Date(2026, 1, 1, 0, 0, 20, 0, time.UTC)
	assert.Equal(t, time.Date(2026, 1, 1, 0, 0, 10, 0, time.UTC), s.Prev(at))
	assert.Equal(t, at, s.Prev(at.Add(time.Nanosecond)))
	// The result is in the location of the given time.
	tokyo, _ := time.LoadLocation("Asia/Tokyo")
	assert.Equal(t, tokyo, s.Prev(at.In(tokyo)).Location())
}

func TestPrevUnsatisfiable(t *testing.T) {
	for _, spec := range []string{"0 0 30 2 *", "TZ=America/New_York * 2 * 3 SUN#2"} {
		sched, err := ParseStandard(spec)
		if err != nil {
			t.Fatal(err)
		}
		assert.True(t, sched.(*SpecSchedule).Prev(time.Now()).IsZero(), spec)
	}
}

// TestPrevAgainstNext checks that Prev is the inverse of Next for random specs: between
// two successive activations a and b (computed by Next), Prev returns a.
func TestPrevAgainstNext(t *testing.T) {
	r := rand.New(rand.NewSource(1))

	check := func(t *testing.T, spec string, sched *SpecSchedule, start, end time.Time) {
		t.Helper()
		var activations []time.Time
		for next := sched.Next(start); !next.IsZero() && next.Before(end); next = sched.Next(next) {
			activations = append(activations, next)
		}
		for i := 1; i < len(activations); i++ {
			a, b := activations[i-1], activations[i]
			gap := b.Sub(a)
			for _, at := range []time.Time{b, a.Add(time.Nanosecond), a.Add(time.Second), a.Add(time.Duration(r.Int63n(int64(gap))) + 1)} {
				if got := sched.Prev(at); !got.Equal(a) {
					t.Fatalf("%s: Prev(%v) = %v, expected %v", spec, at, got, a)
				}
			}
		}
	}

	t.Run("day rules", func(t *testing.T) {
		domItems := []string{"*", "1", "15", "31", "L", "L-3", "LW", "1W", "15W", "31W", "1,L", "*/10"}
		dowItems := []string{"*", "1", "7", "FRI-MON", "MON#1", "fri#5", "5L", "SUNL", "1-5"}
		monthItems := []string{"*", "2", "NOV-FEB", "*/5"}
		start := time.Date(2025, 12, 1, 0, 0, 0, 0, time.UTC)
		end := start.AddDate(4, 0, 0)
		for i := 0; i < 200; i++ {
			spec := fmt.Sprintf("TZ=UTC %d %d %s %s %s", r.Intn(60), r.Intn(24),
				domItems[r.Intn(len(domItems))], monthItems[r.Intn(len(monthItems))], dowItems[r.Intn(len(dowItems))])
			sched, err := ParseStandard(spec)
			if err != nil {
				t.Fatal(err)
			}
			check(t, spec, sched.(*SpecSchedule), start, end)
		}
	})

	t.Run("DST", func(t *testing.T) {
		transitions := []struct{ zone, at string }{
			{"America/New_York", "2026-03-08 07:00 +0000"},
			{"America/New_York", "2026-11-01 06:00 +0000"},
			{"Australia/Lord_Howe", "2026-04-04 15:00 +0000"},
			{"Australia/Lord_Howe", "2026-10-03 15:30 +0000"},
			{"America/Santiago", "2026-04-05 03:00 +0000"},
			{"America/Santiago", "2026-09-06 04:00 +0000"},
			{"Pacific/Apia", "2011-12-30 10:00 +0000"},
		}
		minuteItems := []string{"0", "30", "45", "*", "*/15", "*/20", "0-59/7", "5,35"}
		hourItems := []string{"*", "*/2", "0", "1", "2", "23", "1-3", "0,1"}
		for _, tr := range transitions {
			at, err := time.Parse(dstLayout, tr.at)
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 50; i++ {
				spec := fmt.Sprintf("TZ=%s %s %s * * *", tr.zone, minuteItems[r.Intn(len(minuteItems))], hourItems[r.Intn(len(hourItems))])
				sched, err := ParseStandard(spec)
				if err != nil {
					t.Fatal(err)
				}
				check(t, spec, sched.(*SpecSchedule), at.Add(-26*time.Hour), at.Add(26*time.Hour))
			}
		}
	})
}

func TestNextN(t *testing.T) {
	from := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	sched, _ := ParseStandard("TZ=UTC 0 0 L * *")
	assert.Equal(t, []time.Time{
		time.Date(2026, 1, 31, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 2, 28, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 3, 31, 0, 0, 0, 0, time.UTC),
	}, NextN(sched, from, 3))

	assert.Equal(t, []time.Time{from.Add(time.Hour), from.Add(2 * time.Hour)}, NextN(Every(time.Hour), from, 2))
	assert.Empty(t, NextN(sched, from, 0))
	assert.Empty(t, NextN(sched, from, -1))

	// No (more) activations.
	never, _ := ParseStandard("0 0 30 2 *")
	assert.Empty(t, NextN(never, from, 3))
	assert.Empty(t, NextN(stuckSchedule{}, from, 3))
}

// stuckSchedule is a broken schedule whose Next does not move forward.
type stuckSchedule struct{}

func (stuckSchedule) Next(t time.Time) time.Time { return t }

func TestActivations(t *testing.T) {
	from := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	sched, _ := ParseStandard("TZ=UTC 0 9 * * MON-FRI")
	var got []time.Time
	for next := range Activations(sched, from) {
		if next.After(from.AddDate(0, 0, 7)) {
			break
		}
		got = append(got, next)
	}
	// January 2026: Thursday 1st to Thursday 8th.
	assert.Equal(t, []time.Time{
		time.Date(2026, 1, 1, 9, 0, 0, 0, time.UTC),
		time.Date(2026, 1, 2, 9, 0, 0, 0, time.UTC),
		time.Date(2026, 1, 5, 9, 0, 0, 0, time.UTC),
		time.Date(2026, 1, 6, 9, 0, 0, 0, time.UTC),
		time.Date(2026, 1, 7, 9, 0, 0, 0, time.UTC),
	}, got)
}

func TestCronNextActivations(t *testing.T) {
	tokyo, err := time.LoadLocation("Asia/Tokyo")
	if err != nil {
		t.Fatal(err)
	}
	cron, clock := newRegressionCron(func(b *Builder) { b.WithLocation(tokyo) })
	// 2000-01-01 00:00 UTC is 09:00 in Tokyo.
	id, err := cron.AddJob("0 0 12 * * *", func() {})
	assert.NoError(t, err)

	got, err := cron.NextActivations(id, 3)
	assert.NoError(t, err)
	assert.Equal(t, []time.Time{
		time.Date(2000, 1, 1, 12, 0, 0, 0, tokyo),
		time.Date(2000, 1, 2, 12, 0, 0, 0, tokyo),
		time.Date(2000, 1, 3, 12, 0, 0, 0, tokyo),
	}, got)
	for _, next := range got {
		assert.Equal(t, tokyo, next.Location())
	}

	// The pinned next activation time comes first.
	pinned := clock.Now().Add(time.Minute)
	id2, err := cron.AddJob("0 0 12 * * *", func() {}, WithNext(pinned))
	assert.NoError(t, err)
	got, err = cron.NextActivations(id2, 2)
	assert.NoError(t, err)
	assert.Equal(t, []time.Time{pinned.In(tokyo), time.Date(2000, 1, 1, 12, 0, 0, 0, tokyo)}, got)

	got, err = cron.NextActivations(id, 0)
	assert.NoError(t, err)
	assert.Empty(t, got)

	// A disabled entry has no next activation time: the activations of its schedule from now
	id3, _ := cron.AddJob("0 0 12 * * *", func() {}, Disabled)
	got, err = cron.NextActivations(id3, 1)
	assert.NoError(t, err)
	assert.Equal(t, []time.Time{time.Date(2000, 1, 1, 12, 0, 0, 0, tokyo)}, got)

	_, err = cron.NextActivations("unknown", 3)
	assert.ErrorIs(t, err, ErrEntryNotFound)
}
