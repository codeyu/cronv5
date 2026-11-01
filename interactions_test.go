package cron

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// The tag limits apply to the runs started after SetTags
func TestSetTagsChangesConcurrencyLimit(t *testing.T) {
	cron, _ := newRegressionCron(func(b *Builder) { b.WithTagConcurrencyLimit("serial", 1) })
	cron.Start()
	defer cron.Stop()
	probe := newConcurrencyProbe()
	id, _ := cron.AddJob("0 0 0 1 1 *", probe.job)
	_, _ = cron.Trigger(id)
	_, _ = cron.Trigger(id)
	assert.Eventually(t, func() bool { return probe.current.Load() == 2 }, time.Second, time.Millisecond)
	close(probe.release)
	cron.waitAllJobsCompleted()

	probe2 := newConcurrencyProbe()
	id2, _ := cron.AddJob("0 0 0 1 1 *", probe2.job)
	assert.NoError(t, cron.SetTags(id2, "serial"))
	_, _ = cron.Trigger(id2)
	_, _ = cron.Trigger(id2)
	assert.Eventually(t, func() bool { return probe2.current.Load() == 1 }, time.Second, time.Millisecond)
	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, int32(1), probe2.current.Load())
	close(probe2.release)
	cron.waitAllJobsCompleted()
	assert.Equal(t, int32(1), probe2.max.Load())
}

// MisfireRunAll catching up across a DST transition: the activation of the day the clock
// jumps forward runs at the end of the jump, as Next computes it
func TestMisfireRunAllAcrossDST(t *testing.T) {
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatal(err)
	}
	cron, clock := newRegressionCron(func(b *Builder) { b.WithLocation(ny) })
	clock.Advance(time.Date(2026, 3, 7, 12, 0, 0, 0, ny).Sub(clock.Now()))
	var mu sync.Mutex
	var scheduled []string
	_, _ = cron.AddJob("0 30 2 * * *", func(_ context.Context, _ *Cron, r JobRun) {
		mu.Lock()
		defer mu.Unlock()
		scheduled = append(scheduled, r.ScheduledAt.In(ny).Format(dstLayout))
	}, WithMisfirePolicy(MisfireRunAll))
	clock.Advance(3 * 24 * time.Hour) // stopped until March 10th, noon
	cron.Start()
	defer cron.Stop()
	cycle(cron)
	cron.waitAllJobsCompleted()
	mu.Lock()
	defer mu.Unlock()
	assert.ElementsMatch(t, []string{"2026-03-08 03:00 -0400", "2026-03-09 02:30 -0400", "2026-03-10 02:30 -0400"}, scheduled)
}

// A triggered run waiting for a concurrency limit is registered, not started yet
func TestTriggerWaitingForLimit(t *testing.T) {
	cron, _ := newRegressionCron(func(b *Builder) { b.WithMaxConcurrentJobs(1) })
	cron.Start()
	defer cron.Stop()
	probe := newConcurrencyProbe()
	a, _ := cron.AddJob("0 0 0 1 1 *", probe.job)
	_, _ = cron.Trigger(a)
	assert.Eventually(t, func() bool { return probe.current.Load() == 1 }, time.Second, time.Millisecond)
	b, _ := cron.AddJob("0 0 0 1 1 *", func() {})
	runID, err := cron.Trigger(b)
	assert.NoError(t, err)
	run, err := cron.GetJobRun(b, runID)
	assert.NoError(t, err)
	assert.Nil(t, run.StartedAt)
	assert.True(t, run.Triggered)
	close(probe.release)
	cron.waitAllJobsCompleted()
}
