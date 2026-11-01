package cron

import (
	"runtime/debug"
	"time"
)

// Metrics receives measurements of the job runs, eg: to export them to Prometheus
// (counters of started / failed / skipped runs, histograms of durations and delays).
//
// The methods are called synchronously by the cron, from the goroutine of the job run
// (or the scheduler loop for JobSkipped): they must be fast and safe for concurrent use.
// A panic in a method is recovered and logged.
//
// Implementations should embed BaseMetrics, so that methods added to this interface in the
// future do not break them.
type Metrics interface {
	// JobStarted is called when a job run starts. delay is how late it started compared
	// to its scheduled time (JobRun.ScheduledAt).
	JobStarted(run JobRun, delay time.Duration)
	// JobCompleted is called when a job run completes. JobRun.Error and JobRun.Panic tell how it ended.
	JobCompleted(run JobRun, duration time.Duration)
	// JobSkipped is called when an activation of an entry does not run, see SkipReason.
	JobSkipped(entry Entry, scheduledAt time.Time, reason SkipReason)
}

// SkipReason tells why an activation of an entry did not run
type SkipReason int

const (
	// SkipMisfire the activation was missed by more than the misfire threshold, with the MisfireSkip policy
	SkipMisfire SkipReason = iota + 1
	// SkipStillRunning a previous run of the entry was still running, with the NoOverlap option
	SkipStillRunning
)

func (r SkipReason) String() string {
	switch r {
	case SkipMisfire:
		return "misfire"
	case SkipStillRunning:
		return "still running"
	default:
		return "unknown"
	}
}

// BaseMetrics implements Metrics with methods doing nothing, to embed in Metrics implementations.
type BaseMetrics struct{}

// JobStarted does nothing
func (BaseMetrics) JobStarted(JobRun, time.Duration) {}

// JobCompleted does nothing
func (BaseMetrics) JobCompleted(JobRun, time.Duration) {}

// JobSkipped does nothing
func (BaseMetrics) JobSkipped(Entry, time.Time, SkipReason) {}

// WithMetrics sets the Metrics receiving the measurements of the job runs.
func (b *Builder) WithMetrics(metrics Metrics) *Builder {
	b.metrics = metrics
	return b
}

// withMetrics calls fn with the metrics if there are some, recovering a panic in fn
func (c *Cron) withMetrics(fn func(Metrics)) {
	if c.metrics == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			c.logger.Error("metrics panic", "error", r, "stack", string(debug.Stack()))
		}
	}()
	fn(c.metrics)
}
