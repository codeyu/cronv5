package cron

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTriggerReturnsRunID(t *testing.T) {
	cron, _ := newRegressionCron(func(b *Builder) { b.WithKeepCompletedRunsDur(time.Hour) })
	cron.Start()
	defer cron.Stop()
	gotRunID := make(chan RunID, 1)
	id, _ := cron.AddJob("0 0 0 1 1 *", func(_ context.Context, _ *Cron, r JobRun) { gotRunID <- r.RunID })

	runID, err := cron.Trigger(id)
	assert.NoError(t, err)
	assert.NotEmpty(t, runID)
	cron.waitAllJobsCompleted()
	assert.Equal(t, runID, <-gotRunID)
	run, err := cron.GetJobRun(id, runID)
	assert.NoError(t, err)
	assert.Equal(t, runID, run.RunID)
	assert.NotNil(t, run.CompletedAt)
}

func TestTriggerKeepsSchedule(t *testing.T) {
	cron, clock := newRegressionCron()
	cron.Start()
	defer cron.Stop()
	start := clock.Now()
	id, _ := cron.Schedule(Every(time.Hour), J(func() {}))
	next := start.Add(time.Hour)
	entry, _ := cron.Entry(id)
	assert.Equal(t, next, entry.Next)

	advanceAndCycle(cron, 10*time.Minute)
	_, err := cron.Trigger(id)
	assert.NoError(t, err)
	assert.NoError(t, cron.RunNow(id))
	cron.waitAllJobsCompleted()

	// The planned activation is unchanged, the phase of "@every 1h" is kept
	entry, _ = cron.Entry(id)
	assert.Equal(t, next, entry.Next)
	assert.Equal(t, start.Add(10*time.Minute), entry.Prev)
}

func TestTriggerDisabledEntry(t *testing.T) {
	cron, _ := newRegressionCron()
	cron.Start()
	defer cron.Stop()
	var calls atomic.Int32
	id, _ := cron.AddJob("* * * * * *", func() { calls.Add(1) }, Disabled)
	_, err := cron.Trigger(id)
	assert.NoError(t, err)
	cron.waitAllJobsCompleted()
	assert.Equal(t, int32(1), calls.Load())
	entry, _ := cron.Entry(id)
	assert.False(t, entry.Active)
	assert.True(t, entry.Next.IsZero())
}

func TestTriggerErrors(t *testing.T) {
	cron, _ := newRegressionCron()
	id, _ := cron.AddJob("* * * * * *", func() {})
	_, err := cron.Trigger(id)
	assert.ErrorIs(t, err, ErrCronNotRunning)

	cron.Start()
	_, err = cron.Trigger("unknown")
	assert.ErrorIs(t, err, ErrEntryNotFound)
	assert.ErrorIs(t, cron.RunNow("unknown"), ErrEntryNotFound)

	cron.Stop()
	_, err = cron.Trigger(id)
	assert.ErrorIs(t, err, ErrCronNotRunning)
}

// A triggered run goes through the job wrappers, hooks and panic recovery, like a scheduled one.
func TestTriggerIsLikeAScheduledRun(t *testing.T) {
	cron, _ := newRegressionCron()
	cron.Start()
	defer cron.Stop()

	var errs, panics, completed atomic.Int32
	cron.OnEvt(JobErr, func(context.Context, *Cron, HookID, JobRun) { errs.Add(1) }, HookSync)
	cron.OnEvt(JobPanic, func(context.Context, *Cron, HookID, JobRun) { panics.Add(1) }, HookSync)
	cron.OnEvt(JobCompleted, func(context.Context, *Cron, HookID, JobRun) { completed.Add(1) }, HookSync)

	// Wrapper: the second run is skipped while the first one is running
	release := make(chan struct{})
	id, _ := cron.AddJob("0 0 0 1 1 *", SkipIfStillRunning(func() { <-release }))
	_, err := cron.Trigger(id)
	assert.NoError(t, err)
	assert.True(t, cron.IsRunning(id)) // started when Trigger returns
	runID2, err := cron.Trigger(id)
	assert.NoError(t, err)
	assert.Eventually(t, func() bool { return errs.Load() == 1 }, time.Second, time.Millisecond)
	close(release)
	cron.waitAllJobsCompleted()
	assert.Equal(t, int32(2), completed.Load())

	// Panic recovery
	id2, _ := cron.AddJob("0 0 0 1 1 *", func() { panic("boom") })
	_, err = cron.Trigger(id2)
	assert.NoError(t, err)
	cron.waitAllJobsCompleted()
	assert.Equal(t, int32(1), panics.Load())
	assert.NotEqual(t, RunID(""), runID2)
}

func TestTriggerWithOnce(t *testing.T) {
	cron, _ := newRegressionCron()
	cron.Start()
	defer cron.Stop()
	ch := make(chan error, 1)
	id, _ := cron.AddJob("0 0 0 1 1 *", Once(func(ctx context.Context) { ch <- ctx.Err() }))
	_, err := cron.Trigger(id)
	assert.NoError(t, err)
	assert.NoError(t, recvErrWithTimeout(t, ch))
	cron.waitAllJobsCompleted()
	_, err = cron.Entry(id)
	assert.True(t, errors.Is(err, ErrEntryNotFound))
}
