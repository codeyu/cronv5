package cron

import (
	"iter"
	"time"
)

// Activations returns the successive activation times of the schedule after the given time
// (as computed by Next). The sequence ends if the schedule has no more activations, it is
// infinite otherwise: stop the iteration (break) once enough times are read.
//
//	for t := range cron.Activations(schedule, time.Now()) {
//		if t.After(limit) {
//			break
//		}
//		fmt.Println(t)
//	}
func Activations(s Schedule, from time.Time) iter.Seq[time.Time] {
	return func(yield func(time.Time) bool) {
		for t := s.Next(from); !t.IsZero() && t.After(from); t = s.Next(from) {
			if !yield(t) {
				return
			}
			from = t
		}
	}
}

// NextN returns the next n activation times of the schedule after the given time.
// It returns less than n times if the schedule has no more activations.
func NextN(s Schedule, from time.Time, n int) []time.Time {
	out := make([]time.Time, 0, max(n, 0))
	for t := range Activations(s, from) {
		if len(out) >= n {
			break
		}
		out = append(out, t)
	}
	return out
}

// NextActivations returns the next n times the entry will be activated, in the cron location,
// starting with the entry's next activation time (Entry.Next). It does not take into account
// whether the entry is active, or the job wrappers (eg: a job wrapped with Once runs only once).
func (c *Cron) NextActivations(id EntryID, n int) ([]time.Time, error) {
	entry, err := c.getEntry(id)
	if err != nil {
		return nil, err
	}
	out := make([]time.Time, 0, max(n, 0))
	next := entry.Next
	if next.IsZero() {
		next = entry.Schedule.Next(c.now())
	}
	if n <= 0 || next.IsZero() {
		return out, nil
	}
	loc := c.getLocation()
	out = append(out, next.In(loc))
	for _, t := range NextN(entry.Schedule, next.In(loc), n-1) {
		out = append(out, t.In(loc))
	}
	return out, nil
}
