package cron

import "time"

// SpecSchedule specifies a duty cycle (to the second granularity), based on a
// traditional crontab specification. It is computed initially and stored as bit sets.
type SpecSchedule struct {
	Second, Minute, Hour, Dom, Month, Dow uint64

	// Override location for this schedule.
	Location *time.Location

	// Day rules that cannot be represented as bit sets ("L", "W", "#").
	days dayRules
}

// dayRules holds the day-of-month and day-of-week items that depend on the month
// (its length, or the weekday it starts on). They match in addition to the bits
// set in Dom and Dow.
type dayRules struct {
	lastDom        uint32   // bit n: "L-n" (n days before the last day of the month, "L" is "L-0")
	weekdayDom     uint32   // bit n: "nW" (weekday nearest to the n-th day of the month)
	lastWeekdayDom bool     // "LW" (last weekday of the month)
	nthDow         [7]uint8 // bit k of nthDow[d]: "d#k" (k-th weekday d of the month)
	lastDow        uint8    // bit d: "dL" (last weekday d of the month)
}

func (r dayRules) hasDom() bool { return r.lastDom != 0 || r.weekdayDom != 0 || r.lastWeekdayDom }

func (r dayRules) hasDow() bool { return r.lastDow != 0 || r.nthDow != [7]uint8{} }

// bounds provides a range of acceptable values (plus a map of name to value).
type bounds struct {
	min, max uint
	names    map[string]uint
	// cyclic fields accept ranges that wrap around, eg: "FRI-MON" or "NOV-FEB".
	cyclic bool
	// sunday7 accepts 7 as an alias of 0 (Sunday), as Vixie cron does.
	sunday7 bool
}

// The bounds for each field.
var (
	seconds = bounds{min: 0, max: 59}
	minutes = bounds{min: 0, max: 59}
	hours   = bounds{min: 0, max: 23}
	dom     = bounds{min: 1, max: 31}
	months  = bounds{min: 1, max: 12, cyclic: true, names: map[string]uint{
		"jan": 1,
		"feb": 2,
		"mar": 3,
		"apr": 4,
		"may": 5,
		"jun": 6,
		"jul": 7,
		"aug": 8,
		"sep": 9,
		"oct": 10,
		"nov": 11,
		"dec": 12,
	}}
	dow = bounds{min: 0, max: 6, cyclic: true, sunday7: true, names: map[string]uint{
		"sun": 0,
		"mon": 1,
		"tue": 2,
		"wed": 3,
		"thu": 4,
		"fri": 5,
		"sat": 6,
	}}
)

const (
	// Set the top bit if a star was included in the expression.
	starBit = 1 << 63
)

// Next returns the next time this schedule is activated, greater than the given
// time.  If no time can be found to satisfy the schedule, return the zero time.
func (s *SpecSchedule) Next(t time.Time) time.Time {
	// General approach
	//
	// For Month, Day, Hour, Minute, Second:
	// Check if the time value matches.  If yes, continue to the next field.
	// If the field doesn't match the schedule, then increment the field until it matches.
	// While incrementing the field, a wrap-around brings it back to the beginning
	// of the field list (since it is necessary to re-verify previous field
	// values)

	// Convert the given time into the schedule's timezone, if one is specified.
	// Save the original timezone so we can convert back after we find a time.
	// Note that schedules without a time zone specified (time.Local) are treated
	// as local to the time provided.
	origLocation := t.Location()
	loc := s.Location
	if loc == time.Local {
		loc = t.Location()
	}
	if s.Location != time.Local {
		t = t.In(s.Location)
	}

	// Start at the earliest possible time (the upcoming second).
	t = t.Add(1*time.Second - time.Duration(t.Nanosecond())*time.Nanosecond)

	// This flag indicates whether a field has been incremented.
	added := false

	// If no time is found within five years, return zero.
	yearLimit := t.Year() + 5

WRAP:
	if t.Year() > yearLimit {
		return time.Time{}
	}

	// Find the first applicable month.
	// If it's this month, then do nothing.
	for 1<<uint(t.Month())&s.Month == 0 {
		// If we have to add a month, reset the other parts to 0.
		if !added {
			added = true
			// Otherwise, set the date at the beginning (since the current time is irrelevant).
			t = time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, loc)
		}
		t = t.AddDate(0, 1, 0)

		// Wrapped around.
		if t.Month() == time.January {
			goto WRAP
		}
	}

	// Now get a day in that month.
	//
	// NOTE: This causes issues for daylight savings regimes where midnight does
	// not exist.  For example: Sao Paulo has DST that transforms midnight on
	// 11/3 into 1am. Handle that by noticing when the Hour ends up != 0.
	for !dayMatches(s, t) {
		if !added {
			added = true
			t = time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, loc)
		}
		t = t.AddDate(0, 0, 1)
		// Notice if the hour is no longer midnight due to DST.
		// Add an hour if it's 23, subtract an hour if it's 1.
		if t.Hour() != 0 {
			if t.Hour() > 12 {
				t = t.Add(time.Duration(24-t.Hour()) * time.Hour)
			} else {
				t = t.Add(time.Duration(-t.Hour()) * time.Hour)
			}
		}

		if t.Day() == 1 {
			goto WRAP
		}
	}

	for 1<<uint(t.Hour())&s.Hour == 0 {
		if !added {
			added = true
			t = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, loc)
		}
		t = t.Add(1 * time.Hour)

		if t.Hour() == 0 {
			goto WRAP
		}
	}

	for 1<<uint(t.Minute())&s.Minute == 0 {
		if !added {
			added = true
			t = t.Truncate(time.Minute)
		}
		t = t.Add(1 * time.Minute)

		if t.Minute() == 0 {
			goto WRAP
		}
	}

	for 1<<uint(t.Second())&s.Second == 0 {
		if !added {
			added = true
			t = t.Truncate(time.Second)
		}
		t = t.Add(1 * time.Second)

		if t.Second() == 0 {
			goto WRAP
		}
	}

	return t.In(origLocation)
}

// dayMatches returns true if the schedule's day-of-week and day-of-month
// restrictions are satisfied by the given time.
func dayMatches(s *SpecSchedule, t time.Time) bool {
	var (
		domMatch = 1<<uint(t.Day())&s.Dom > 0 || (s.days.hasDom() && s.days.domMatches(t))
		dowMatch = 1<<uint(t.Weekday())&s.Dow > 0 || (s.days.hasDow() && s.days.dowMatches(t))
	)
	if s.Dom&starBit > 0 || s.Dow&starBit > 0 {
		return domMatch && dowMatch
	}
	return domMatch || dowMatch
}

// daysIn returns the number of days in the month of t.
func daysIn(t time.Time) int {
	return time.Date(t.Year(), t.Month()+1, 0, 0, 0, 0, 0, time.UTC).Day()
}

// weekdayOf returns the weekday of the given day of the month of t.
func weekdayOf(t time.Time, day int) time.Weekday {
	return time.Date(t.Year(), t.Month(), day, 0, 0, 0, 0, time.UTC).Weekday()
}

// nearestWeekday returns the weekday (Monday to Friday) nearest to the given day,
// without leaving the month (Quartz "W" semantics): Saturday the 1st gives Monday
// the 3rd, Sunday the 31st gives Friday the 29th.
func nearestWeekday(t time.Time, day, last int) int {
	switch weekdayOf(t, day) {
	case time.Saturday:
		if day == 1 {
			return day + 2
		}
		return day - 1
	case time.Sunday:
		if day == last {
			return day - 2
		}
		return day + 1
	}
	return day
}

func (r dayRules) domMatches(t time.Time) bool {
	day, last := t.Day(), daysIn(t)
	if offset := last - day; offset < 32 && r.lastDom&(1<<offset) != 0 {
		return true
	}
	if r.lastWeekdayDom && nearestWeekdayBefore(t, last) == day {
		return true
	}
	// The weekday nearest to n is at most 2 days away from n.
	for n := max(1, day-2); n <= min(last, day+2); n++ {
		if r.weekdayDom&(1<<n) != 0 && nearestWeekday(t, n, last) == day {
			return true
		}
	}
	return false
}

// nearestWeekdayBefore returns the last weekday (Monday to Friday) on or before the given day.
func nearestWeekdayBefore(t time.Time, day int) int {
	switch weekdayOf(t, day) {
	case time.Saturday:
		return day - 1
	case time.Sunday:
		return day - 2
	}
	return day
}

func (r dayRules) dowMatches(t time.Time) bool {
	wd, day := t.Weekday(), t.Day()
	if r.nthDow[wd]&(1<<((day-1)/7+1)) != 0 {
		return true
	}
	return r.lastDow&(1<<wd) != 0 && day+7 > daysIn(t)
}
