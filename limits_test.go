package cron

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// concurrencyProbe is a job blocking until released, recording how many run at the same time
type concurrencyProbe struct {
	current, max, done atomic.Int32
	release            chan struct{}
}

func newConcurrencyProbe() *concurrencyProbe {
	return &concurrencyProbe{release: make(chan struct{})}
}

func (p *concurrencyProbe) job(ctx context.Context) error {
	n := p.current.Add(1)
	for {
		m := p.max.Load()
		if n <= m || p.max.CompareAndSwap(m, n) {
			break
		}
	}
	defer p.current.Add(-1)
	defer p.done.Add(1)
	select {
	case <-p.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestMaxConcurrentJobs(t *testing.T) {
	cron, _ := newRegressionCron(func(b *Builder) { b.WithMaxConcurrentJobs(2) })
	cron.Start()
	defer cron.Stop()
	probe := newConcurrencyProbe()
	for i := 0; i < 5; i++ {
		id, _ := cron.AddJob("0 0 0 1 1 *", probe.job)
		_, err := cron.Trigger(id)
		assert.NoError(t, err)
	}
	assert.Eventually(t, func() bool { return probe.current.Load() == 2 }, time.Second, time.Millisecond)
	time.Sleep(20 * time.Millisecond) // the other runs keep waiting
	assert.Equal(t, int32(2), probe.current.Load())
	assert.Len(t, cron.RunningJobs(), 5) // waiting runs are registered as running
	close(probe.release)
	cron.waitAllJobsCompleted()
	assert.Equal(t, int32(5), probe.done.Load())
	assert.Equal(t, int32(2), probe.max.Load())
}

func TestTagConcurrencyLimit(t *testing.T) {
	cron, _ := newRegressionCron(func(b *Builder) { b.WithTagConcurrencyLimit("db", 1).WithTagConcurrencyLimit("unlimited", 0) })
	cron.Start()
	defer cron.Stop()
	db, other := newConcurrencyProbe(), newConcurrencyProbe()
	for i := 0; i < 3; i++ {
		id, _ := cron.AddJob("0 0 0 1 1 *", db.job, WithTags("db", "unlimited"))
		_, _ = cron.Trigger(id)
		id, _ = cron.AddJob("0 0 0 1 1 *", other.job, WithTags("unlimited"))
		_, _ = cron.Trigger(id)
	}
	assert.Eventually(t, func() bool { return db.current.Load() == 1 && other.current.Load() == 3 }, time.Second, time.Millisecond)
	close(other.release)
	close(db.release)
	cron.waitAllJobsCompleted()
	assert.Equal(t, int32(1), db.max.Load())
	assert.Equal(t, int32(3), other.max.Load())
	assert.Equal(t, int32(3), db.done.Load())
}

// Runs waiting for several limits do not deadlock, whatever the order of the tags
func TestConcurrencyLimitsNoDeadlock(t *testing.T) {
	cron, _ := newRegressionCron(func(b *Builder) {
		b.WithMaxConcurrentJobs(3).WithTagConcurrencyLimit("a", 1).WithTagConcurrencyLimit("b", 1)
	})
	cron.Start()
	defer cron.Stop()
	var calls atomic.Int32
	var ids []EntryID
	for i, tags := range [][]string{{"a", "b"}, {"b", "a"}, {"a"}, {"b"}, {"b", "a", "c"}} {
		id, _ := cron.AddJob("0 0 0 1 1 *", func() { calls.Add(1); time.Sleep(time.Millisecond) }, WithTags(tags...), WithID(EntryID(fmt.Sprint(i))))
		ids = append(ids, id)
	}
	for i := 0; i < 20; i++ {
		for _, id := range ids {
			_, _ = cron.Trigger(id)
		}
	}
	done := make(chan struct{})
	go func() { cron.waitAllJobsCompleted(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("deadlock")
	}
	assert.Equal(t, int32(100), calls.Load())
}

// A run cancelled while waiting for a concurrency limit completes without starting the job
func TestConcurrencyLimitCancelledWhileWaiting(t *testing.T) {
	cron, _ := newRegressionCron(func(b *Builder) { b.WithMaxConcurrentJobs(1).WithKeepCompletedRunsDur(time.Hour) })
	cron.Start()
	defer cron.Stop()
	probe := newConcurrencyProbe()
	var completed sync.WaitGroup
	completed.Add(1)
	cron.OnEvt(JobCompleted, func(_ context.Context, _ *Cron, _ HookID, r JobRun) {
		if r.Entry.ID == "waiting" {
			completed.Done()
		}
	}, HookSync)
	blocking, _ := cron.AddJob("0 0 0 1 1 *", probe.job)
	_, _ = cron.Trigger(blocking)
	assert.Eventually(t, func() bool { return probe.current.Load() == 1 }, time.Second, time.Millisecond)

	var ran atomic.Bool
	waiting, _ := cron.AddJob("0 0 0 1 1 *", func() { ran.Store(true) }, WithID("waiting"))
	runID, err := cron.Trigger(waiting)
	assert.NoError(t, err)
	assert.True(t, cron.IsRunning(waiting))
	assert.NoError(t, cron.CancelJobRun(waiting, runID))
	completed.Wait()

	run, err := cron.GetJobRun(waiting, runID)
	assert.NoError(t, err)
	assert.ErrorIs(t, run.Error, context.Canceled)
	assert.Nil(t, run.StartedAt)
	assert.NotNil(t, run.CompletedAt)
	var types []JobEventType
	for _, evt := range run.Events {
		types = append(types, evt.Typ)
	}
	assert.Equal(t, []JobEventType{JobErr, JobCompleted}, types)
	assert.False(t, ran.Load())

	// The slot is still usable
	close(probe.release)
	cron.waitAllJobsCompleted()
	ok := make(chan struct{})
	id, _ := cron.AddJob("0 0 0 1 1 *", func() { close(ok) })
	_, _ = cron.Trigger(id)
	select {
	case <-ok:
	case <-time.After(time.Second):
		t.Fatal("the slot was not released")
	}
}

// Stop cancels the runs waiting for a concurrency limit
func TestConcurrencyLimitStop(t *testing.T) {
	cron, _ := newRegressionCron(func(b *Builder) { b.WithMaxConcurrentJobs(1) })
	cron.Start()
	probe := newConcurrencyProbe()
	var ran atomic.Bool
	a, _ := cron.AddJob("0 0 0 1 1 *", probe.job)
	b, _ := cron.AddJob("0 0 0 1 1 *", func() { ran.Store(true) })
	_, _ = cron.Trigger(a)
	assert.Eventually(t, func() bool { return probe.current.Load() == 1 }, time.Second, time.Millisecond)
	_, _ = cron.Trigger(b)
	select {
	case <-cron.Stop():
	case <-time.After(time.Second):
		t.Fatal("Stop did not complete")
	}
	assert.False(t, ran.Load())
}

func TestNoOverlap(t *testing.T) {
	metrics := &skipMetrics{}
	cron, clock := newRegressionCron(func(b *Builder) { b.WithMetrics(metrics) })
	cron.Start()
	defer cron.Stop()
	start := clock.Now()
	probe := newConcurrencyProbe()
	id, _ := cron.AddJob("* * * * * *", probe.job, NoOverlap, WithID("job"))
	entry, _ := cron.Entry(id)
	assert.True(t, entry.NoOverlap)

	advanceAndCycleNoWait(cron, time.Second)
	assert.Eventually(t, func() bool { return probe.current.Load() == 1 }, time.Second, time.Millisecond)
	advanceAndCycleNoWait(cron, time.Second)
	advanceAndCycleNoWait(cron, time.Second)
	_, err := cron.Trigger(id)
	assert.ErrorIs(t, err, ErrJobAlreadyRunning)
	assert.Len(t, cron.RunningJobs(), 1)

	var skipped []time.Time
	for _, m := range metrics.get() {
		if m.kind == "skipped" {
			assert.Equal(t, SkipStillRunning, m.reason)
			skipped = append(skipped, m.schedule)
		}
	}
	assert.Equal(t, []time.Time{start.Add(2 * time.Second), start.Add(3 * time.Second), start.Add(3 * time.Second)}, skipped)

	// Once the run completed, the next activation runs
	close(probe.release)
	cron.waitAllJobsCompleted()
	advanceAndCycle(cron, time.Second)
	assert.Equal(t, int32(2), probe.done.Load())
}

// With MisfireRunAll, only one of the missed activations runs at a time
func TestNoOverlapRunAll(t *testing.T) {
	cron, clock := newRegressionCron()
	var scheduled []time.Time
	var mu sync.Mutex
	_, _ = cron.AddJob("0 * * * * *", func(_ context.Context, _ *Cron, r JobRun) {
		mu.Lock()
		scheduled = append(scheduled, r.ScheduledAt)
		mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}, NoOverlap, WithMisfirePolicy(MisfireRunAll))
	start := clock.Now()
	clock.Advance(5 * time.Minute)
	cron.Start()
	defer cron.Stop()
	cycle(cron)
	cron.waitAllJobsCompleted()
	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []time.Time{start.Add(time.Minute)}, slices.Clone(scheduled))
}
