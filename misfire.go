package cron

import "time"

// MisfirePolicy tells what to do with the activations of an entry that were missed: the cron
// was not running at that time, or could not run the job on time (eg: the machine was sleeping).
type MisfirePolicy int

const (
	// MisfireDefault uses the policy of the cron (Builder.WithMisfirePolicy) for an entry.
	// As the policy of the cron, the activations missed while the cron is running run once
	// (like MisfireRunOnce), and those missed while it is not running are skipped.
	MisfireDefault MisfirePolicy = iota
	// MisfireSkip skips the activations missed by more than the misfire threshold
	// (Builder.WithMisfireThreshold), the entry runs at its next activation.
	MisfireSkip
	// MisfireRunOnce runs the job once for all the missed activations, as soon as possible.
	MisfireRunOnce
	// MisfireRunAll runs the job once for each missed activation, as soon as possible
	// (JobRun.ScheduledAt is the activation each run is for). With a frequent schedule, this
	// can be a lot of runs: wrap the job with DelayIfStillRunning to run them one at a time.
	MisfireRunAll
)

// defaultMisfireThreshold is the default delay after which an activation is missed (as Quartz)
const defaultMisfireThreshold = time.Minute

// maxRunsPerPass limits the runs started by one pass of the scheduler loop, so that the entries
// lock is not held for too long (eg: MisfireRunAll catching up many activations)
const maxRunsPerPass = 1000

func (p MisfirePolicy) String() string {
	switch p {
	case MisfireDefault:
		return "default"
	case MisfireSkip:
		return "skip"
	case MisfireRunOnce:
		return "run once"
	case MisfireRunAll:
		return "run all"
	default:
		return "unknown"
	}
}

// WithMisfirePolicy sets the misfire policy of the entry (see MisfirePolicy).
// Activations explicitly set (RunOnStart, WithNext, AddJobAt, RunNow before Start) are never skipped.
func WithMisfirePolicy(policy MisfirePolicy) EntryOption {
	return func(_ *Cron, entry *Entry) {
		entry.MisfirePolicy = policy
	}
}

// WithMisfirePolicy sets the misfire policy of the entries using MisfireDefault (see MisfirePolicy).
func (b *Builder) WithMisfirePolicy(policy MisfirePolicy) *Builder {
	b.misfirePolicy = policy
	return b
}

// WithMisfireThreshold sets how late an activation must be to be missed, for the MisfireSkip policy
// (default: one minute).
func (b *Builder) WithMisfireThreshold(threshold time.Duration) *Builder {
	b.misfireThreshold = &threshold
	return b
}

func (c *Cron) misfirePolicyOf(entry *Entry) MisfirePolicy {
	if entry.MisfirePolicy != MisfireDefault {
		return entry.MisfirePolicy
	}
	return c.misfirePolicy
}

type skippedActivation struct {
	entry       Entry
	scheduledAt time.Time
	reason      SkipReason
}

// reportSkipped logs the skipped activations and reports them to the metrics (without holding a lock)
func (c *Cron) reportSkipped(skipped []skippedActivation) {
	for _, s := range skipped {
		c.logger.Info("activation skipped", "label", s.entry.Label, "entryID", s.entry.ID, "scheduledAt", s.scheduledAt, "reason", s.reason.String())
		c.withMetrics(func(m Metrics) { m.JobSkipped(s.entry, s.scheduledAt, s.reason) })
	}
}
