package cron

import (
	"slices"
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
	// Tags of the entry (see WithTags), to query or manage entries by group
	Tags []string
	// Skip the activations happening while a previous run of the entry is still running (see NoOverlap)
	NoOverlap bool
	// What to do with the activations missed by more than the misfire threshold (see WithMisfirePolicy).
	// MisfireDefault uses the policy of the cron.
	MisfirePolicy MisfirePolicy
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

// snapshot returns a copy of the entry that does not share memory with it
func (e *Entry) snapshot() Entry {
	out := *e
	out.Tags = slices.Clone(e.Tags)
	return out
}

// HasTag returns true if the entry has the given tag
func (e Entry) HasTag(tag string) bool {
	return slices.Contains(e.Tags, tag)
}

type EntryOption func(*Cron, *Entry)

// WithTags adds tags to the entry, to query or manage entries by group
// (EntriesByTag, RemoveByTag, EnableByTag, DisableByTag) or to limit their concurrency
// (WithTagConcurrencyLimit). Duplicated and empty tags are ignored.
func WithTags(tags ...string) EntryOption {
	return func(_ *Cron, entry *Entry) {
		entry.Tags = mergeTags(entry.Tags, tags)
	}
}

// mergeTags returns a new slice with the tags of a then b, without duplicated or empty tags
func mergeTags(a, b []string) []string {
	out := make([]string, 0, len(a)+len(b))
	for _, tag := range slices.Concat(a, b) {
		if tag != "" && !slices.Contains(out, tag) {
			out = append(out, tag)
		}
	}
	return out
}

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

// NoOverlap skips the activations of the entry happening while a previous run of the entry is
// still running (including a run waiting for a concurrency limit). The skipped activations are
// reported to Metrics.JobSkipped, and Trigger returns ErrJobAlreadyRunning.
//
// Unlike the SkipIfStillRunning job wrapper, no job run is created for a skipped activation.
func NoOverlap(_ *Cron, entry *Entry) {
	entry.NoOverlap = true
}

func Disabled(_ *Cron, entry *Entry) {
	entry.Active = false
}
