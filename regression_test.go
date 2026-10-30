package cron

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
)

// Regression tests for the bugs listed in TODO.md (P0 section)

func newRegressionCron(opts ...func(*Builder)) (*Cron, *clockwork.FakeClock) {
	clock := clockwork.NewFakeClockAt(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC))
	b := New().WithClock(clock).WithParser(secondParser).WithLogger(newNoOpLogger())
	for _, opt := range opts {
		opt(b)
	}
	return b.Build(), clock
}

func recvErrWithTimeout(t *testing.T, ch <-chan error) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(time.Second):
		t.Fatal("timeout")
		return nil
	}
}

// B1: Once/N must not cancel the context of the run that removes the entry
func TestOnceDoesNotCancelItsOwnRun(t *testing.T) {
	cron, _ := newRegressionCron()
	cron.Start()
	defer cron.Stop()
	ch := make(chan error, 1)
	id, _ := cron.AddJob("* * * * * *", Once(func(ctx context.Context) { ch <- ctx.Err() }))
	advanceAndCycle(cron, time.Second)
	assert.NoError(t, recvErrWithTimeout(t, ch))
	_, err := cron.Entry(id)
	assert.ErrorIs(t, err, ErrEntryNotFound)
}

func TestNDoesNotCancelItsLastRun(t *testing.T) {
	cron, _ := newRegressionCron()
	cron.Start()
	defer cron.Stop()
	ch := make(chan error, 2)
	id, _ := cron.AddJob("* * * * * *", N(2, func(ctx context.Context) { ch <- ctx.Err() }))
	advanceAndCycle(cron, time.Second)
	advanceAndCycle(cron, time.Second)
	assert.NoError(t, recvErrWithTimeout(t, ch))
	assert.NoError(t, recvErrWithTimeout(t, ch))
	_, err := cron.Entry(id)
	assert.ErrorIs(t, err, ErrEntryNotFound)
}

// B2: Wait/Stop must not block forever when the cron is not running
func TestWaitWithoutStart(t *testing.T) {
	cron, _ := newRegressionCron()
	done := make(chan struct{})
	go func() {
		cron.Wait()
		<-cron.Stop()
		close(done)
	}()
	recvWithTimeout(t, done, "Wait should return when the cron is not running")
}

func TestWaitAfterStop(t *testing.T) {
	cron, _ := newRegressionCron()
	cron.Start()
	<-cron.Stop()
	done := make(chan struct{})
	go func() {
		cron.Wait()
		close(done)
	}()
	recvWithTimeout(t, done, "Wait should return when the cron is already stopped")
}

// B3: cron can be restarted after being stopped
func TestRestartAfterStop(t *testing.T) {
	var calls atomic.Int32
	cron, _ := newRegressionCron()
	_, _ = cron.AddJob("* * * * * *", baseJob{&calls})
	cron.Start()
	advanceAndCycle(cron, time.Second)
	assert.Equal(t, int32(1), calls.Load())
	<-cron.Stop()
	assert.True(t, cron.Start())
	defer cron.Stop()
	assert.NoError(t, cron.ctx().Err())
	advanceAndCycle(cron, time.Second)
	assert.Equal(t, int32(2), calls.Load())
}

// B4: a sync hook can use the hooks API without deadlocking
func TestSyncHookCanUseHooksAPI(t *testing.T) {
	cron, _ := newRegressionCron()
	cron.Start()
	defer cron.Stop()
	done := make(chan struct{})
	cron.OnJobStart(func(_ context.Context, c *Cron, id HookID, _ JobRun) {
		c.SetHookLabel(id, "label")
		c.RemoveHook(id)
		close(done)
	}, HookSync)
	_, _ = cron.AddJob("* * * * * *", func() {})
	advanceAndCycleNoWait(cron, time.Second)
	recvWithTimeout(t, done, "sync hook deadlocked")
	cron.waitAllJobsCompleted()
	assert.Empty(t, cron.GetHooks())
}

// B5: hooksFor must not write into the backing array of the global hooks slice
func TestHooksForDoesNotAlias(t *testing.T) {
	hc := hooksContainer{
		hooksMap:       make(map[HookID]hookMeta),
		globalHooksMap: make(map[JobEventType][]*hookStruct),
		entryHooksMap:  make(map[EntryID]map[JobEventType][]*hookStruct),
	}
	hc.globalHooksMap[JobStart] = make([]*hookStruct, 1, 4) // Spare capacity, as left by slices.Delete
	hc.globalHooksMap[JobStart][0] = &hookStruct{id: "global"}
	hc.addEntryHook("a", JobStart, &hookStruct{id: "a"})
	hc.addEntryHook("b", JobStart, &hookStruct{id: "b"})
	hooksA := hc.hooksFor(JobStart, "a")
	hooksB := hc.hooksFor(JobStart, "b")
	assert.Equal(t, HookID("a"), hooksA[1].id)
	assert.Equal(t, HookID("b"), hooksB[1].id)
}

// B4/B5: hooks of an entry are not run for another entry, with concurrent jobs (run with -race)
func TestEntryHooksConcurrentJobs(t *testing.T) {
	cron, _ := newRegressionCron()
	cron.Start()
	defer cron.Stop()
	cron.OnJobStart(func(context.Context, *Cron, HookID, JobRun) {}, HookSync)
	var wrongEntry atomic.Int32
	for i := 0; i < 20; i++ {
		id, _ := cron.AddJob("* * * * * *", func() {})
		cron.OnEntryJobStart(id, func(_ context.Context, _ *Cron, _ HookID, jr JobRun) {
			if jr.Entry.ID != id {
				wrongEntry.Add(1)
			}
		}, HookSync)
	}
	for i := 0; i < 5; i++ {
		advanceAndCycle(cron, time.Second)
	}
	assert.Equal(t, int32(0), wrongEntry.Load())
}

// B6: reading the logs of a running job while it writes them (run with -race)
func TestJobRunLogsConcurrentRead(t *testing.T) {
	cron, _ := newRegressionCron()
	cron.Start()
	defer cron.Stop()
	done := make(chan struct{})
	id, _ := cron.AddJob("* * * * * *", func(r JobRun) {
		defer close(done)
		for i := 0; i < 100; i++ {
			r.Logger().Info("hello")
		}
	}, RunOnStart)
	cycle(cron)
	for {
		select {
		case <-done:
			cron.waitAllJobsCompleted()
			return
		default:
			for _, jr := range ignoreErr(cron.RunningJobsFor(id)) {
				_ = jr.Logs
			}
		}
	}
}

func ignoreErr[T any](v T, _ error) T { return v }

// B7: an exported JobRun is a snapshot, it must not share memory with the run
func TestJobRunExportIsSnapshot(t *testing.T) {
	clock := clockwork.NewFakeClock()
	jr := newJobRun(context.Background(), clock, Entry{ID: "id"}, UuidIDFactory(), DefaultJobRunLoggerFactory())
	jr.inner.With(func(inner *jobRunInner) { inner.addEvent(newJobEvent(JobStart, clock)) })
	snapshot := jr.export()
	jr.inner.With(func(inner *jobRunInner) {
		inner.events = inner.events[:0]
		inner.addEvent(newJobEvent(JobErr, clock))
	})
	assert.Equal(t, JobStart, snapshot.Events[0].Typ)
}

// B8: WithRetry returns the error of the last attempt
func TestWithRetryReturnsLastError(t *testing.T) {
	cron, clock := newRegressionCron()
	var attempts atomic.Int32
	job := WithRetry(1, func() error { return errors.New("err" + string(rune('0'+attempts.Add(1)))) })
	ch := make(chan error, 1)
	go func() { ch <- job.Run(context.Background(), cron, JobRun{}) }()
	clock.BlockUntil(1) // Waiting before the retry
	clock.Advance(time.Second)
	assert.EqualError(t, recvErrWithTimeout(t, ch), "err2")
	assert.Equal(t, int32(2), attempts.Load())
}

func TestWithRetryNoRetryNeeded(t *testing.T) {
	cron, _ := newRegressionCron()
	var attempts atomic.Int32
	job := WithRetry(3, func() error { attempts.Add(1); return nil })
	assert.NoError(t, job.Run(context.Background(), cron, JobRun{}))
	assert.Equal(t, int32(1), attempts.Load())
}

func TestWithRetryNoWaitAfterLastAttempt(t *testing.T) {
	cron, _ := newRegressionCron()
	job := WithRetry(0, func() error { return errors.New("boom") })
	ch := make(chan error, 1)
	go func() { ch <- job.Run(context.Background(), cron, JobRun{}) }()
	assert.EqualError(t, recvErrWithTimeout(t, ch), "boom")
}

func TestWithRetryStopsWhenCancelled(t *testing.T) {
	cron, clock := newRegressionCron()
	var attempts atomic.Int32
	job := WithRetry(10, func() error { attempts.Add(1); return errors.New("boom") })
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan error, 1)
	go func() { ch <- job.Run(ctx, cron, JobRun{}) }()
	clock.BlockUntil(1)
	cancel()
	assert.EqualError(t, recvErrWithTimeout(t, ch), "boom")
	assert.Equal(t, int32(1), attempts.Load())
}

// B9: ThresholdClbAsync is called only when the job runs longer than the threshold
func TestThresholdClbAsyncSlowJob(t *testing.T) {
	cron, clock := newRegressionCron()
	called := make(chan struct{})
	release := make(chan struct{})
	job := ThresholdClbAsync(time.Minute, func() { <-release }, func(context.Context, *Cron, JobRun, time.Duration) {
		close(called)
	})
	done := make(chan error, 1)
	go func() { done <- job.Run(context.Background(), cron, JobRun{}) }()
	clock.BlockUntil(1)
	clock.Advance(time.Minute)
	recvWithTimeout(t, called, "callback should be called for a slow job")
	close(release)
	assert.NoError(t, recvErrWithTimeout(t, done))
}

func TestThresholdClbAsyncFastJob(t *testing.T) {
	cron, clock := newRegressionCron()
	var calls atomic.Int32
	job := ThresholdClbAsync(time.Minute, func() {}, func(context.Context, *Cron, JobRun, time.Duration) {
		calls.Add(1)
	})
	assert.NoError(t, job.Run(context.Background(), cron, JobRun{}))
	clock.Advance(2 * time.Minute)
	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, int32(0), calls.Load())
}

// B10: the job context is cancelled when the job completes, hooks still get a live context
func TestJobContextCancelledOnCompletion(t *testing.T) {
	cron, _ := newRegressionCron(func(b *Builder) { b.WithKeepCompletedRunsDur(time.Hour) })
	cron.Start()
	defer cron.Stop()
	jobCtxCh := make(chan context.Context, 1)
	hookErrCh := make(chan error, 1)
	jobDone := make(chan struct{})
	cron.OnJobCompleted(func(ctx context.Context, _ *Cron, _ HookID, _ JobRun) {
		<-jobDone
		hookErrCh <- ctx.Err()
	})
	_, _ = cron.AddJob("* * * * * *", func(ctx context.Context) { jobCtxCh <- ctx })
	advanceAndCycle(cron, time.Second)
	close(jobDone)
	assert.Error(t, (<-jobCtxCh).Err())
	assert.NoError(t, recvErrWithTimeout(t, hookErrCh))
	assert.Len(t, cron.CompletedJobs(), 1)
}

// B11: entries added before Start are not run immediately because their Next is in the past
func TestStaleEntriesNotRunOnStart(t *testing.T) {
	var calls, onStartCalls atomic.Int32
	cron, clock := newRegressionCron()
	_, _ = cron.AddJob("0 0 * * * *", baseJob{&calls}) // Hourly
	_, _ = cron.AddJob("0 0 * * * *", baseJob{&onStartCalls}, RunOnStart)
	clock.Advance(90 * time.Minute)
	cron.Start()
	defer cron.Stop()
	cycle(cron)
	cron.waitAllJobsCompleted()
	assert.Equal(t, int32(0), calls.Load())
	assert.Equal(t, int32(1), onStartCalls.Load())
	assert.Equal(t, time.Date(2000, 1, 1, 2, 0, 0, 0, time.UTC), cron.GetNextTime())
	advanceAndCycle(cron, 30*time.Minute)
	assert.Equal(t, int32(1), calls.Load())
	assert.Equal(t, int32(2), onStartCalls.Load())
}

func TestRunNowBeforeStartIsKept(t *testing.T) {
	var calls atomic.Int32
	cron, clock := newRegressionCron()
	id, _ := cron.AddJob("0 0 * * * *", baseJob{&calls})
	assert.NoError(t, cron.RunNow(id))
	clock.Advance(time.Minute)
	cron.Start()
	defer cron.Stop()
	cycle(cron)
	cron.waitAllJobsCompleted()
	assert.Equal(t, int32(1), calls.Load())
}

// B12: Entry.Job only unwraps the library wrappers
type singleExportedFieldJob struct{ Name string }

func (singleExportedFieldJob) Run(context.Context, *Cron, JobRun) error { return nil }

type singleFieldJob1 struct{ Name string }

func (singleFieldJob1) Run() {}

func TestEntryJobReturnsUserJob(t *testing.T) {
	cron, _ := newRegressionCron()
	id1, _ := cron.AddJob("* * * * * *", singleExportedFieldJob{Name: "x"})
	id2, _ := cron.AddJob("* * * * * *", singleFieldJob1{Name: "y"})
	e1, _ := cron.Entry(id1)
	e2, _ := cron.Entry(id2)
	assert.Equal(t, singleExportedFieldJob{Name: "x"}, e1.Job())
	assert.Equal(t, singleFieldJob1{Name: "y"}, e2.Job())
}

// B13: removing a running entry does not leak its job runs nor its hooks
func TestRemoveRunningEntryDoesNotLeak(t *testing.T) {
	cron, _ := newRegressionCron(func(b *Builder) { b.WithKeepCompletedRunsDur(time.Hour) })
	cron.Start()
	defer cron.Stop()
	started := make(chan struct{})
	release := make(chan struct{})
	id, _ := cron.AddJob("* * * * * *", func() {
		close(started)
		<-release
	}, RunOnStart)
	cron.OnEntryJobCompleted(id, func(context.Context, *Cron, HookID, JobRun) {})
	cycle(cron)
	<-started
	cron.Remove(id)
	close(release)
	cron.waitAllJobsCompleted()
	_, ok := cron.runningJobsMap.Load(id)
	assert.False(t, ok)
	assert.Empty(t, cron.GetHooks())
	assert.Empty(t, cron.CompletedJobs())
}

// B14: adding the same ID concurrently succeeds only once
func TestAddEntryConcurrentSameID(t *testing.T) {
	cron, _ := newRegressionCron()
	var wg sync.WaitGroup
	var success atomic.Int32
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := cron.AddJob("* * * * * *", func() {}, WithID("same")); err == nil {
				success.Add(1)
			}
		}()
	}
	wg.Wait()
	assert.Equal(t, int32(1), success.Load())
	assert.Len(t, cron.Entries(), 1)
}

func TestAddEntryValidation(t *testing.T) {
	cron, _ := newRegressionCron()
	_, err := cron.Schedule(nil, J(func() {}))
	assert.Error(t, err)
	_, err = cron.Schedule(Every(time.Second), nil)
	assert.Error(t, err)
	_, err = cron.AddEntry(Entry{ID: "id", Schedule: Every(time.Second)})
	assert.Error(t, err)
	assert.Empty(t, cron.Entries())
}

// B16: completed runs are cleaned up after changing the interval, and after a restart
func TestCleanupAfterIntervalChangeAndRestart(t *testing.T) {
	cron, clock := newRegressionCron()
	cron.Start()
	cron.SetCleanupInterval(0)
	cron.SetCleanupInterval(time.Minute)
	cron.SetCleanupInterval(0)
	cron.SetCleanupInterval(time.Minute)
	_, _ = cron.AddJob("* * * * * *", func() {}, WithID("job"))
	advanceAndCycle(cron, time.Second)
	cron.Disable("job")
	assert.Len(t, cron.CompletedJobs(), 1)
	<-cron.Stop()
	cron.Start()
	defer cron.Stop()
	clock.Advance(2 * time.Minute)
	cron.CleanupNow()
	assert.Eventually(t, func() bool { return len(cron.CompletedJobs()) == 0 }, time.Second, time.Millisecond)
}

func TestSetCleanupIntervalZeroFreesCompletedRuns(t *testing.T) {
	cron, _ := newRegressionCron(func(b *Builder) { b.WithKeepCompletedRunsDur(time.Hour) })
	cron.Start()
	defer cron.Stop()
	_, _ = cron.AddJob("* * * * * *", func() {})
	advanceAndCycle(cron, time.Second)
	assert.Len(t, cron.CompletedJobs(), 1)
	cron.SetCleanupInterval(0)
	assert.Eventually(t, func() bool { return len(cron.CompletedJobs()) == 0 }, time.Second, time.Millisecond)
}
