package cron

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// misfireRecorder records the ScheduledAt of the runs of a job
type misfireRecorder struct {
	mu        sync.Mutex
	scheduled []time.Time
}

func (r *misfireRecorder) job(_ context.Context, _ *Cron, run JobRun) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.scheduled = append(r.scheduled, run.ScheduledAt)
}

func (r *misfireRecorder) get() []time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := slices.Clone(r.scheduled)
	slices.SortFunc(out, time.Time.Compare)
	return out
}

func everyMinute(start time.Time, from, to int) (out []time.Time) {
	for i := from; i <= to; i++ {
		out = append(out, start.Add(time.Duration(i)*time.Minute))
	}
	return
}

// The scheduler is late (eg: the machine was sleeping): the clock jumps 10 minutes at once
func TestMisfireWhileRunning(t *testing.T) {
	for _, tt := range []struct {
		name     string
		builder  MisfirePolicy
		entry    MisfirePolicy
		expected func(start time.Time) []time.Time
		skipped  int
	}{
		{"default", MisfireDefault, MisfireDefault, func(s time.Time) []time.Time { return everyMinute(s, 1, 1) }, 0},
		{"run once", MisfireDefault, MisfireRunOnce, func(s time.Time) []time.Time { return everyMinute(s, 1, 1) }, 0},
		{"skip", MisfireDefault, MisfireSkip, func(time.Time) []time.Time { return nil }, 1},
		{"run all", MisfireDefault, MisfireRunAll, func(s time.Time) []time.Time { return everyMinute(s, 1, 10) }, 0},
		{"cron policy", MisfireRunAll, MisfireDefault, func(s time.Time) []time.Time { return everyMinute(s, 1, 10) }, 0},
		{"entry overrides cron", MisfireRunAll, MisfireSkip, func(time.Time) []time.Time { return nil }, 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			metrics := &skipMetrics{}
			cron, clock := newRegressionCron(func(b *Builder) { b.WithMisfirePolicy(tt.builder).WithMetrics(metrics) })
			start := clock.Now()
			rec := &misfireRecorder{}
			_, _ = cron.AddJob("0 * * * * *", rec.job, WithID("job"), WithMisfirePolicy(tt.entry))
			cron.Start()
			defer cron.Stop()
			advanceAndCycle(cron, 10*time.Minute)
			assert.Equal(t, tt.expected(start), rec.get())

			var skipped []recordedMetric
			for _, m := range metrics.get() {
				if m.kind == "skipped" {
					skipped = append(skipped, m)
				}
			}
			if assert.Len(t, skipped, tt.skipped) && tt.skipped > 0 {
				assert.Equal(t, start.Add(time.Minute), skipped[0].schedule)
				assert.Equal(t, SkipMisfire, skipped[0].reason)
				assert.Equal(t, EntryID("job"), skipped[0].entryID)
			}

			// Back on time: the next activation runs normally
			entry, _ := cron.Entry("job")
			assert.Equal(t, start.Add(11*time.Minute), entry.Next)
			before := len(rec.get())
			advanceAndCycle(cron, time.Minute)
			assert.Len(t, rec.get(), before+1)
		})
	}
}

func TestMisfireSkipThreshold(t *testing.T) {
	cron, clock := newRegressionCron(func(b *Builder) { b.WithMisfireThreshold(30 * time.Second) })
	start := clock.Now()
	rec := &misfireRecorder{}
	_, _ = cron.AddJob("0 * * * * *", rec.job, WithMisfirePolicy(MisfireSkip))
	cron.Start()
	defer cron.Stop()
	advanceAndCycle(cron, 90*time.Second) // 30s late: not missed
	assert.Equal(t, everyMinute(start, 1, 1), rec.get())
	advanceAndCycle(cron, 61*time.Second) // 31s late: missed
	assert.Equal(t, everyMinute(start, 1, 1), rec.get())
}

// The activations went by while the cron was not running
func TestMisfireOnStart(t *testing.T) {
	for _, tt := range []struct {
		name     string
		policy   MisfirePolicy
		stopped  time.Duration
		expected func(start time.Time) []time.Time
	}{
		{"default", MisfireDefault, 10 * time.Minute, func(time.Time) []time.Time { return nil }},
		{"default within threshold", MisfireDefault, 90 * time.Second, func(time.Time) []time.Time { return nil }},
		{"skip", MisfireSkip, 10 * time.Minute, func(time.Time) []time.Time { return nil }},
		{"skip within threshold", MisfireSkip, 90 * time.Second, func(s time.Time) []time.Time { return everyMinute(s, 1, 1) }},
		{"run once", MisfireRunOnce, 10*time.Minute + 30*time.Second, func(s time.Time) []time.Time { return everyMinute(s, 1, 1) }},
		{"run all", MisfireRunAll, 10*time.Minute + 30*time.Second, func(s time.Time) []time.Time { return everyMinute(s, 1, 10) }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			metrics := &skipMetrics{}
			cron, clock := newRegressionCron(func(b *Builder) { b.WithMetrics(metrics) })
			start := clock.Now()
			rec := &misfireRecorder{}
			_, _ = cron.AddJob("0 * * * * *", rec.job, WithMisfirePolicy(tt.policy))
			clock.Advance(tt.stopped)
			cron.Start()
			defer cron.Stop()
			cycle(cron)
			cron.waitAllJobsCompleted()
			assert.Equal(t, tt.expected(start), rec.get())
			skips := 0
			for _, m := range metrics.get() {
				if m.kind == "skipped" {
					skips++
				}
			}
			assert.Equal(t, len(tt.expected(start)) == 0, skips == 1)
		})
	}
}

// Activations explicitly set are never skipped
func TestMisfireSkipKeepsPinnedActivations(t *testing.T) {
	cron, clock := newRegressionCron(func(b *Builder) { b.WithMisfirePolicy(MisfireSkip) })
	var calls atomic.Int32
	_, _ = cron.AddJob("0 0 0 1 1 *", func() { calls.Add(1) }, WithNext(clock.Now().Add(time.Minute)))
	_, _ = cron.AddJob("0 0 0 1 1 *", func() { calls.Add(1) }, RunOnStart)
	_, _ = cron.AddJobAt(clock.Now().Add(time.Minute), func() { calls.Add(1) })
	clock.Advance(time.Hour)
	cron.Start()
	defer cron.Stop()
	cycle(cron)
	cron.waitAllJobsCompleted()
	assert.Equal(t, int32(3), calls.Load())
}

// More missed activations than the runs started by one pass of the scheduler loop
func TestMisfireRunAllManyActivations(t *testing.T) {
	cron, clock := newRegressionCron()
	start := clock.Now()
	var calls atomic.Int32
	_, _ = cron.AddJob("* * * * * *", func() { calls.Add(1) }, WithMisfirePolicy(MisfireRunAll), WithID("job"))
	clock.Advance(2500 * time.Second)
	cron.Start()
	defer cron.Stop()
	cycle(cron)
	assert.Eventually(t, func() bool { return calls.Load() == 2500 }, 5*time.Second, time.Millisecond)
	cron.waitAllJobsCompleted()
	entry, _ := cron.Entry("job")
	assert.Equal(t, start.Add(2501*time.Second), entry.Next)
	assert.Equal(t, start.Add(2500*time.Second), entry.Prev)
	assert.Equal(t, int32(2500), calls.Load())
}

// RunAll with a constant delay schedule
func TestMisfireRunAllEvery(t *testing.T) {
	cron, clock := newRegressionCron()
	start := clock.Now()
	rec := &misfireRecorder{}
	_, _ = cron.Schedule(Every(time.Minute), FuncJob(func(ctx context.Context, c *Cron, r JobRun) error {
		rec.job(ctx, c, r)
		return nil
	}), WithMisfirePolicy(MisfireRunAll))
	cron.Start()
	defer cron.Stop()
	advanceAndCycle(cron, 5*time.Minute+10*time.Second)
	assert.Equal(t, everyMinute(start, 1, 5), rec.get())
}

func TestMisfirePolicyString(t *testing.T) {
	assert.Equal(t, "default", MisfireDefault.String())
	assert.Equal(t, "skip", MisfireSkip.String())
	assert.Equal(t, "run once", MisfireRunOnce.String())
	assert.Equal(t, "run all", MisfireRunAll.String())
	assert.Equal(t, "unknown", MisfirePolicy(42).String())
}
