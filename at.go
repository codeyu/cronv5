package cron

import (
	"errors"
	"time"
)

// AtSchedule is a schedule activating once, at a given time.
type AtSchedule struct {
	Time time.Time
}

// At returns a schedule activating once, at the given time.
//
// Added with Cron.Schedule, the entry stays after its activation (with no next activation
// time): use Cron.AddJobAt to run a job once and remove its entry.
func At(t time.Time) AtSchedule {
	return AtSchedule{Time: t}
}

// Next returns the time of the schedule if it is after the given time, the zero time otherwise.
func (s AtSchedule) Next(t time.Time) time.Time {
	if s.Time.After(t) {
		return s.Time.In(t.Location())
	}
	return time.Time{}
}

// AddJobAt adds a job run once, at the given time. The entry is removed when the job starts.
//
// If the time is already past, the job runs as soon as possible (when the cron is running).
// It also runs when the cron is started, if its time went by while the cron was not running.
// An entry disabled (see Disable) when its time comes does not run, and stays until removed.
// It returns an error if the time is zero.
func (c *Cron) AddJobAt(at time.Time, job IntoJob, opts ...EntryOption) (EntryID, error) {
	return c.addJobAt(at, job, opts...)
}

func (c *Cron) addJobAt(at time.Time, job IntoJob, opts ...EntryOption) (EntryID, error) {
	if at.IsZero() {
		return "", errors.New("time cannot be zero")
	}
	// The time is pinned (as with WithNext): it is not dropped when the cron starts after it.
	opts = append([]EntryOption{WithNext(at)}, opts...)
	return c.schedule(nil, At(at), Once(job), opts...)
}
