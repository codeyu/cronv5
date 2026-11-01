package cron

import (
	"time"
)

// EntryID identifies a cron entry
type EntryID ID

// Entry consists of a schedule and the func to execute on that schedule.
type Entry struct {
	ID   EntryID
	Spec *string
	// The schedule on which this job should be run.
	Schedule Schedule
	// The next time the job will run. This is the zero time if Cron has not been started or this entry's schedule is unsatisfiable
	Next time.Time
	// The last time this job was run. This is the zero time if the job has never been run.
	Prev time.Time
	// Label to describe the job
	Label string
	// Either or not the job is currently active
	Active bool
	// The Job to run.
	job Job
	// Next was explicitly set (RunOnStart, WithNext, RunNow) and must not be recomputed when the cron starts
	pinnedNext bool
}

// jobUnwrapper is implemented by the wrappers the library uses to turn an IntoJob into a Job
type jobUnwrapper interface {
	unwrapJob() any
}

// Job returns the job given to AddJob (for a function, the function itself, before it
// was adapted to the Job interface).
//
// Calling it directly bypasses the cron: no job run (RunID, logger), hooks, events nor panic
// recovery. Use Cron.Trigger (or RunNow) to run the job now as the cron does.
func (e Entry) Job() any {
	if w, ok := e.job.(jobUnwrapper); ok {
		return w.unwrapJob()
	}
	return e.job
}

type EntryOption func(*Cron, *Entry)

func Label(label string) func(*Cron, *Entry) {
	return func(_ *Cron, entry *Entry) {
		entry.Label = label
	}
}

func WithID(id EntryID) func(*Cron, *Entry) {
	return func(_ *Cron, entry *Entry) {
		entry.ID = id
	}
}

func WithNext(next time.Time) func(*Cron, *Entry) {
	return func(_ *Cron, entry *Entry) {
		entry.Next = next
		entry.pinnedNext = true
	}
}

func RunOnStart(c *Cron, entry *Entry) {
	entry.Next = c.now()
	entry.pinnedNext = true
}

func Disabled(_ *Cron, entry *Entry) {
	entry.Active = false
}
