package cron

import (
	"context"
	"errors"
	"io"
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
	jr := newJobRun(context.Background(), clock, Entry{ID: "id"}, UuidIDFactory(), DefaultJobRunLoggerFactory(), 0)
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

// Regression tests for the P1 section of TODO.md

// R2: KeepJobsRunningOnStop lets the running jobs complete
func TestKeepJobsRunningOnStop(t *testing.T) {
	cron, _ := newRegressionCron(func(b *Builder) { b.KeepJobsRunningOnStop() })
	cron.Start()
	started := make(chan struct{})
	release := make(chan struct{})
	ctxErr := make(chan error, 1)
	_, _ = cron.AddJob("* * * * * *", func(ctx context.Context) {
		close(started)
		<-release
		ctxErr <- ctx.Err()
	}, RunOnStart)
	cycle(cron)
	<-started
	stopped := cron.Stop()
	close(release)
	assert.NoError(t, recvErrWithTimeout(t, ctxErr))
	recvWithTimeout(t, stopped, "Stop should complete once the job is done")
}

func TestStopCancelsJobsByDefault(t *testing.T) {
	cron, _ := newRegressionCron()
	cron.Start()
	started := make(chan struct{})
	ctxErr := make(chan error, 1)
	_, _ = cron.AddJob("* * * * * *", func(ctx context.Context) {
		close(started)
		<-ctx.Done()
		ctxErr <- ctx.Err()
	}, RunOnStart)
	cycle(cron)
	<-started
	recvWithTimeout(t, cron.Stop(), "Stop should complete")
	assert.ErrorIs(t, recvErrWithTimeout(t, ctxErr), context.Canceled)
}

func TestShutdownWaitsForJobs(t *testing.T) {
	cron, _ := newRegressionCron()
	cron.Start()
	started := make(chan struct{})
	release := make(chan struct{})
	ctxErr := make(chan error, 1)
	_, _ = cron.AddJob("* * * * * *", func(ctx context.Context) {
		close(started)
		<-release
		ctxErr <- ctx.Err()
	}, RunOnStart)
	cycle(cron)
	<-started
	shutdownErr := make(chan error, 1)
	go func() { shutdownErr <- cron.Shutdown(context.Background()) }()
	close(release)
	assert.NoError(t, recvErrWithTimeout(t, ctxErr))
	assert.NoError(t, recvErrWithTimeout(t, shutdownErr))
	assert.False(t, cron.isRunning())
}

func TestShutdownTimeoutCancelsJobs(t *testing.T) {
	cron, _ := newRegressionCron()
	cron.Start()
	started := make(chan struct{})
	ctxErr := make(chan error, 1)
	_, _ = cron.AddJob("* * * * * *", func(ctx context.Context) {
		close(started)
		<-ctx.Done()
		ctxErr <- ctx.Err()
	}, RunOnStart)
	cycle(cron)
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	assert.ErrorIs(t, cron.Shutdown(ctx), context.DeadlineExceeded)
	assert.ErrorIs(t, recvErrWithTimeout(t, ctxErr), context.Canceled)
	cron.waitAllJobsCompleted()
}

// R3: specs that never activate are rejected
func TestUnsatisfiableSpecRejected(t *testing.T) {
	cron, _ := newRegressionCron()
	_, err := cron.AddJob("0 0 0 30 2 *", func() {})
	assert.ErrorIs(t, err, ErrUnsatisfiableSchedule)
	id, _ := cron.AddJob("0 0 0 * * *", func() {})
	assert.ErrorIs(t, cron.UpdateScheduleWithSpec(id, "0 0 0 31 4 *"), ErrUnsatisfiableSchedule)
	_, err = cron.AddJob("0 0 0 29 2 *", func() {}) // Leap day is fine
	assert.NoError(t, err)
}

// R4: empty time zone is an error
func TestEmptyTimeZoneRejected(t *testing.T) {
	_, err := ParseStandard("TZ= * * * * *")
	assert.Error(t, err)
	_, err = ParseStandard("CRON_TZ= * * * * *")
	assert.Error(t, err)
	_, err = ParseStandard("CRON_TZ=UTC * * * * *")
	assert.NoError(t, err)
}

// R5: logs kept for a job run are capped
func TestMaxLogSize(t *testing.T) {
	buf := &syncBuffer{max: 10}
	n, err := buf.Write([]byte("0123456789abcdef"))
	assert.NoError(t, err)
	assert.Equal(t, 16, n)
	assert.Equal(t, logsTruncatedMsg+"6789abcdef", buf.String())
	_, _ = buf.Write([]byte("XY"))
	assert.Equal(t, logsTruncatedMsg+"89abcdefXY", buf.String())

	unlimited := &syncBuffer{}
	_, _ = unlimited.Write([]byte("0123456789abcdef"))
	assert.Equal(t, "0123456789abcdef", unlimited.String())
}

func TestMaxLogSizeOption(t *testing.T) {
	cron, _ := newRegressionCron(func(b *Builder) { b.WithMaxLogSize(100).WithKeepCompletedRunsDur(time.Hour) })
	cron.Start()
	defer cron.Stop()
	_, _ = cron.AddJob("* * * * * *", func(jr JobRun) {
		for i := 0; i < 100; i++ {
			jr.Logger().Info("some log line")
		}
	})
	advanceAndCycle(cron, time.Second)
	runs := cron.CompletedJobs()
	assert.Len(t, runs, 1)
	assert.LessOrEqual(t, len(runs[0].Logs), 100+len(logsTruncatedMsg))
}

// R6: number of completed runs kept per entry is capped
func TestMaxCompletedRunsPerEntry(t *testing.T) {
	cron, _ := newRegressionCron(func(b *Builder) { b.WithMaxCompletedRunsPerEntry(3) })
	cron.Start()
	defer cron.Stop()
	id, _ := cron.AddJob("* * * * * *", func() {})
	var runIDs []RunID
	cron.OnEntryJobStart(id, func(_ context.Context, _ *Cron, _ HookID, jr JobRun) { runIDs = append(runIDs, jr.RunID) }, HookSync)
	for i := 0; i < 5; i++ {
		advanceAndCycle(cron, time.Second)
	}
	runs, _ := cron.CompletedJobRunsFor(id)
	assert.Len(t, runs, 3)
	assert.Equal(t, runIDs[2:], []RunID{runs[0].RunID, runs[1].RunID, runs[2].RunID})
	_, err := cron.GetJobRun(id, runIDs[0])
	assert.ErrorIs(t, err, ErrJobRunNotFound)
	// Cleanup by age is not applied when only the max number of runs is set
	cron.CleanupNow()
	time.Sleep(10 * time.Millisecond)
	runs, _ = cron.CompletedJobRunsFor(id)
	assert.Len(t, runs, 3)
}

// R7: wrappers reject unsupported job types when they are created
func TestWrappersPanicOnCreation(t *testing.T) {
	assert.Panics(t, func() { Once(1) })
	assert.Panics(t, func() { WithRetry(1, "not a job") })
	assert.Panics(t, func() { WithTimeout(time.Second, 1) })
	assert.Panics(t, func() { SkipIfStillRunning(1) })
}

// R11: N requires a positive number
func TestNRequiresPositive(t *testing.T) {
	assert.Panics(t, func() { N(0, func() {}) })
	assert.Panics(t, func() { N(-1, func() {}) })
	assert.NotPanics(t, func() { N(1, func() {}) })
}

// Tests for the P2 section of TODO.md

// P2/P4: the scheduler does not hold the entries lock while calling user factories,
// and AddJob does not wait for the scheduler loop.
func TestAddJobDoesNotWaitForSchedulerLoop(t *testing.T) {
	factoryCalled := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	loggerFactory := FuncJobRunLoggerFactory(func(w io.Writer) Logger {
		once.Do(func() {
			close(factoryCalled)
			<-release // Block the scheduler loop
		})
		return DiscardLogger
	})
	cron, clock := newRegressionCron(func(b *Builder) { b.WithJobRunLoggerFactory(loggerFactory) })
	_, _ = cron.AddJob("* * * * * *", func() {})
	cron.Start()
	defer cron.Stop()
	go clock.Advance(time.Second)
	<-factoryCalled
	done := make(chan struct{})
	go func() {
		_, _ = cron.AddJob("* * * * * *", func() {})
		_ = cron.Entries()
		cron.Disable(cron.Entries()[0].ID)
		close(done)
	}()
	recvWithTimeout(t, done, "AddJob/Entries/Disable should not wait for the scheduler loop")
	close(release)
}

func TestRunNowStartsJobBeforeReturning(t *testing.T) {
	cron, _ := newRegressionCron()
	cron.Start()
	defer cron.Stop()
	release := make(chan struct{})
	id, _ := cron.AddJob("0 0 0 1 1 *", func() { <-release })
	assert.NoError(t, cron.RunNow(id))
	assert.True(t, cron.IsRunning(id))
	close(release)
}

// P3: Entries are sorted by next activation time, inactive entries last
func TestEntriesSorted(t *testing.T) {
	cron, _ := newRegressionCron()
	_, _ = cron.Schedule(Every(3*time.Second), J(func() {}), WithID("3s"))
	_, _ = cron.Schedule(Every(time.Second), J(func() {}), WithID("disabled"), Disabled)
	_, _ = cron.Schedule(Every(time.Second), J(func() {}), WithID("1s"))
	_, _ = cron.Schedule(Every(2*time.Second), J(func() {}), WithID("2s"))
	var ids []EntryID
	for _, e := range cron.Entries() {
		ids = append(ids, e.ID)
	}
	assert.Equal(t, []EntryID{"1s", "2s", "3s", "disabled"}, ids)
}

// P5: cleanup frees old runs of every entry
func TestCleanupOldRunsAllEntries(t *testing.T) {
	cron, clock := newRegressionCron(func(b *Builder) { b.WithKeepCompletedRunsDur(time.Minute) })
	cron.Start()
	defer cron.Stop()
	for i := 0; i < 50; i++ {
		_, _ = cron.AddJob("* * * * * *", func() {})
	}
	advanceAndCycle(cron, time.Second)
	assert.Len(t, cron.CompletedJobs(), 50)
	for _, e := range cron.Entries() {
		cron.Disable(e.ID) // No new runs while advancing the clock
	}
	clock.Advance(2 * time.Minute)
	cron.cleanupOldRuns(cron.ctx(), time.Minute)
	assert.Empty(t, cron.CompletedJobs())
}

// P6: locations are cached
func TestLoadLocationCached(t *testing.T) {
	loc1, err := loadLocation("Asia/Tokyo")
	assert.NoError(t, err)
	loc2, _ := loadLocation("Asia/Tokyo")
	assert.Same(t, loc1, loc2)
	_, err = loadLocation("Invalid/Zone")
	assert.Error(t, err)
}

// P7: logs string is cached until the next write
func TestSyncBufferStringCached(t *testing.T) {
	buf := &syncBuffer{}
	_, _ = buf.Write([]byte("a"))
	assert.Equal(t, "a", buf.String())
	assert.Equal(t, "a", buf.String())
	_, _ = buf.Write([]byte("b"))
	assert.Equal(t, "ab", buf.String())
	assert.Equal(t, 0.0, testing.AllocsPerRun(10, func() { _ = buf.String() }))
}

func TestRecentCompletedJobs(t *testing.T) {
	cron, _ := newRegressionCron(func(b *Builder) { b.WithKeepCompletedRunsDur(time.Hour) })
	cron.Start()
	defer cron.Stop()
	_, _ = cron.AddJob("* * * * * *", func() {}, WithID("a"))
	_, _ = cron.AddJob("*/2 * * * * *", func() {}, WithID("b"))
	for i := 0; i < 4; i++ {
		advanceAndCycle(cron, time.Second)
	}
	all := cron.CompletedJobs() // Oldest first
	assert.Len(t, all, 6)
	recent := cron.RecentCompletedJobs(3) // Newest first
	assert.Len(t, recent, 3)
	for i := 1; i < len(recent); i++ {
		assert.False(t, recent[i].CreatedAt.After(recent[i-1].CreatedAt))
	}
	assert.Equal(t, all[len(all)-1].CreatedAt, recent[0].CreatedAt)
	assert.Len(t, cron.RecentCompletedJobs(0), 6)
}
