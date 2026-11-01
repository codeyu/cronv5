package cron

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jonboulle/clockwork"

	"github.com/codeyu/cronv5/internal/utils"
)

// Job is an interface for submitted cron jobs.
type Job interface {
	Run(context.Context, *Cron, JobRun) error
}

type job1 interface{ Run() }
type job2 interface{ Run(context.Context) }
type job3 interface{ Run(EntryID) }
type job4 interface {
	Run(context.Context, EntryID)
}
type job5 interface{ Run() error }
type job6 interface{ Run(context.Context) error }
type job7 interface{ Run(EntryID) error }
type job8 interface {
	Run(context.Context, EntryID) error
}
type job9 interface{ Run(*Cron) }
type job10 interface{ Run(*Cron) error }
type job11 interface{ Run(context.Context, *Cron) }
type job12 interface {
	Run(context.Context, *Cron) error
}
type job13 interface{ Run(*Cron, EntryID) }
type job14 interface{ Run(*Cron, EntryID) error }
type job15 interface {
	Run(context.Context, *Cron, EntryID)
}
type job16 interface {
	Run(context.Context, *Cron, EntryID) error
}
type job17 interface{ Run(Entry) }
type job18 interface{ Run(Entry) error }
type job19 interface{ Run(context.Context, Entry) }
type job20 interface {
	Run(context.Context, Entry) error
}
type job21 interface{ Run(*Cron, Entry) }
type job22 interface{ Run(*Cron, Entry) error }
type job23 interface {
	Run(context.Context, *Cron, Entry)
}
type job24 interface{ Run(JobRun) }
type job25 interface{ Run(JobRun) error }
type job26 interface{ Run(context.Context, JobRun) }
type job27 interface {
	Run(context.Context, JobRun) error
}
type job28 interface{ Run(*Cron, JobRun) }
type job29 interface{ Run(*Cron, JobRun) error }
type job30 interface {
	Run(context.Context, *Cron, JobRun)
}
type job31 interface {
	Run(context.Context, *Cron, Entry) error
}

// FuncJob is a wrapper that turns a func() into a cron.Job
type FuncJob func(context.Context, *Cron, JobRun) error

func (f FuncJob) Run(ctx context.Context, c *Cron, r JobRun) error { return f(ctx, c, r) }

type job1Wrapper struct{ job1 }

func (j *job1Wrapper) unwrapJob() any { return j.job1 }

func (j *job1Wrapper) Run(context.Context, *Cron, JobRun) error {
	j.job1.Run()
	return nil
}

type job2Wrapper struct{ job2 }

func (j *job2Wrapper) unwrapJob() any { return j.job2 }

func (j *job2Wrapper) Run(ctx context.Context, _ *Cron, _ JobRun) error {
	j.job2.Run(ctx)
	return nil
}

type job3Wrapper struct{ job3 }

func (j *job3Wrapper) unwrapJob() any { return j.job3 }

func (j *job3Wrapper) Run(_ context.Context, _ *Cron, r JobRun) error {
	j.job3.Run(r.Entry.ID)
	return nil
}

type job4Wrapper struct{ job4 }

func (j *job4Wrapper) unwrapJob() any { return j.job4 }

func (j *job4Wrapper) Run(ctx context.Context, _ *Cron, r JobRun) error {
	j.job4.Run(ctx, r.Entry.ID)
	return nil
}

type job5Wrapper struct{ job5 }

func (j *job5Wrapper) unwrapJob() any { return j.job5 }

func (j *job5Wrapper) Run(context.Context, *Cron, JobRun) error { return j.job5.Run() }

type job6Wrapper struct{ job6 }

func (j *job6Wrapper) unwrapJob() any { return j.job6 }

func (j *job6Wrapper) Run(ctx context.Context, _ *Cron, _ JobRun) error {
	return j.job6.Run(ctx)
}

type job7Wrapper struct{ job7 }

func (j *job7Wrapper) unwrapJob() any { return j.job7 }

func (j *job7Wrapper) Run(_ context.Context, _ *Cron, r JobRun) error {
	return j.job7.Run(r.Entry.ID)
}

type job8Wrapper struct{ job8 }

func (j *job8Wrapper) unwrapJob() any { return j.job8 }

func (j *job8Wrapper) Run(ctx context.Context, _ *Cron, r JobRun) error {
	return j.job8.Run(ctx, r.Entry.ID)
}

type job9Wrapper struct{ job9 }

func (j *job9Wrapper) unwrapJob() any { return j.job9 }

func (j *job9Wrapper) Run(_ context.Context, cron *Cron, _ JobRun) error {
	j.job9.Run(cron)
	return nil
}

type job10Wrapper struct{ job10 }

func (j *job10Wrapper) unwrapJob() any { return j.job10 }

func (j *job10Wrapper) Run(_ context.Context, cron *Cron, _ JobRun) error {
	return j.job10.Run(cron)
}

type job11Wrapper struct{ job11 }

func (j *job11Wrapper) unwrapJob() any { return j.job11 }

func (j *job11Wrapper) Run(ctx context.Context, cron *Cron, _ JobRun) error {
	j.job11.Run(ctx, cron)
	return nil
}

type job12Wrapper struct{ job12 }

func (j *job12Wrapper) unwrapJob() any { return j.job12 }

func (j *job12Wrapper) Run(ctx context.Context, cron *Cron, _ JobRun) error {
	return j.job12.Run(ctx, cron)
}

type job13Wrapper struct{ job13 }

func (j *job13Wrapper) unwrapJob() any { return j.job13 }

func (j *job13Wrapper) Run(_ context.Context, cron *Cron, r JobRun) error {
	j.job13.Run(cron, r.Entry.ID)
	return nil
}

type job14Wrapper struct{ job14 }

func (j *job14Wrapper) unwrapJob() any { return j.job14 }

func (j *job14Wrapper) Run(_ context.Context, cron *Cron, r JobRun) error {
	return j.job14.Run(cron, r.Entry.ID)
}

type job15Wrapper struct{ job15 }

func (j *job15Wrapper) unwrapJob() any { return j.job15 }

func (j *job15Wrapper) Run(ctx context.Context, cron *Cron, r JobRun) error {
	j.job15.Run(ctx, cron, r.Entry.ID)
	return nil
}

type job16Wrapper struct{ job16 }

func (j *job16Wrapper) unwrapJob() any { return j.job16 }

func (j *job16Wrapper) Run(ctx context.Context, cron *Cron, r JobRun) error {
	return j.job16.Run(ctx, cron, r.Entry.ID)
}

type job17Wrapper struct{ job17 }

func (j *job17Wrapper) unwrapJob() any { return j.job17 }

func (j *job17Wrapper) Run(_ context.Context, _ *Cron, r JobRun) error {
	j.job17.Run(r.Entry)
	return nil
}

type job18Wrapper struct{ job18 }

func (j *job18Wrapper) unwrapJob() any { return j.job18 }

func (j *job18Wrapper) Run(_ context.Context, _ *Cron, r JobRun) error {
	return j.job18.Run(r.Entry)
}

type job19Wrapper struct{ job19 }

func (j *job19Wrapper) unwrapJob() any { return j.job19 }

func (j *job19Wrapper) Run(ctx context.Context, _ *Cron, r JobRun) error {
	j.job19.Run(ctx, r.Entry)
	return nil
}

type job20Wrapper struct{ job20 }

func (j *job20Wrapper) unwrapJob() any { return j.job20 }

func (j *job20Wrapper) Run(ctx context.Context, _ *Cron, r JobRun) error {
	return j.job20.Run(ctx, r.Entry)
}

type job21Wrapper struct{ job21 }

func (j *job21Wrapper) unwrapJob() any { return j.job21 }

func (j *job21Wrapper) Run(_ context.Context, c *Cron, r JobRun) error {
	j.job21.Run(c, r.Entry)
	return nil
}

type job22Wrapper struct{ job22 }

func (j *job22Wrapper) unwrapJob() any { return j.job22 }

func (j *job22Wrapper) Run(_ context.Context, c *Cron, r JobRun) error {
	return j.job22.Run(c, r.Entry)
}

type job23Wrapper struct{ job23 }

func (j *job23Wrapper) unwrapJob() any { return j.job23 }

func (j *job23Wrapper) Run(ctx context.Context, c *Cron, r JobRun) error {
	j.job23.Run(ctx, c, r.Entry)
	return nil
}

type job24Wrapper struct{ job24 }

func (j *job24Wrapper) unwrapJob() any { return j.job24 }

func (j *job24Wrapper) Run(_ context.Context, _ *Cron, r JobRun) error {
	j.job24.Run(r)
	return nil
}

type job25Wrapper struct{ job25 }

func (j *job25Wrapper) unwrapJob() any { return j.job25 }

func (j *job25Wrapper) Run(_ context.Context, _ *Cron, r JobRun) error {
	return j.job25.Run(r)
}

type job26Wrapper struct{ job26 }

func (j *job26Wrapper) unwrapJob() any { return j.job26 }

func (j *job26Wrapper) Run(ctx context.Context, _ *Cron, r JobRun) error {
	j.job26.Run(ctx, r)
	return nil
}

type job27Wrapper struct{ job27 }

func (j *job27Wrapper) unwrapJob() any { return j.job27 }

func (j *job27Wrapper) Run(ctx context.Context, _ *Cron, r JobRun) error {
	return j.job27.Run(ctx, r)
}

type job28Wrapper struct{ job28 }

func (j *job28Wrapper) unwrapJob() any { return j.job28 }

func (j *job28Wrapper) Run(_ context.Context, c *Cron, r JobRun) error {
	j.job28.Run(c, r)
	return nil
}

type job29Wrapper struct{ job29 }

func (j *job29Wrapper) unwrapJob() any { return j.job29 }

func (j *job29Wrapper) Run(_ context.Context, c *Cron, r JobRun) error {
	return j.job29.Run(c, r)
}

type job30Wrapper struct{ job30 }

func (j *job30Wrapper) unwrapJob() any { return j.job30 }

func (j *job30Wrapper) Run(ctx context.Context, c *Cron, r JobRun) error {
	j.job30.Run(ctx, c, r)
	return nil
}

type job31Wrapper struct{ job31 }

func (j *job31Wrapper) unwrapJob() any { return j.job31 }

func (j *job31Wrapper) Run(ctx context.Context, c *Cron, r JobRun) error {
	return j.job31.Run(ctx, c, r.Entry)
}

// IntoJob is something that can be cast into a Job.
// See the J helper documentation for all accepted function signatures.
type IntoJob any

// J is a helper to turn a IntoJob into a Job. It panics with ErrUnsupportedJobType for other types.
// A Job, any of these functions, or a value having a "Run" method with one of these signatures
// (eg: "func (MyJob) Run(context.Context) error") can be turned into a Job:
//
//	func()
//	func() error
//	func(context.Context)
//	func(context.Context) error
//	func(cron.EntryID)
//	func(cron.EntryID) error
//	func(cron.Entry)
//	func(cron.Entry) error
//	func(*cron.Cron)
//	func(*cron.Cron) error
//	func(cron.JobRun)
//	func(cron.JobRun) error
//	func(context.Context, cron.EntryID)
//	func(context.Context, cron.EntryID) error
//	func(context.Context, cron.Entry)
//	func(context.Context, cron.Entry) error
//	func(context.Context, *cron.Cron)
//	func(context.Context, *cron.Cron) error
//	func(context.Context, cron.JobRun)
//	func(context.Context, cron.JobRun) error
//	func(*cron.Cron, cron.EntryID)
//	func(*cron.Cron, cron.EntryID) error
//	func(*cron.Cron, cron.Entry)
//	func(*cron.Cron, cron.Entry) error
//	func(*cron.Cron, cron.JobRun)
//	func(*cron.Cron, cron.JobRun) error
//	func(context.Context, *cron.Cron, cron.EntryID)
//	func(context.Context, *cron.Cron, cron.EntryID) error
//	func(context.Context, *cron.Cron, cron.Entry)
//	func(context.Context, *cron.Cron, cron.Entry) error
//	func(context.Context, *cron.Cron, cron.JobRun)
//	func(context.Context, *cron.Cron, cron.JobRun) error
func J(v IntoJob) Job { return castIntoJob(v) }

func castIntoJob(v IntoJob) Job {
	switch j := v.(type) {
	case func():
		return FuncJob(func(context.Context, *Cron, JobRun) error {
			j()
			return nil
		})
	case func() error:
		return FuncJob(func(context.Context, *Cron, JobRun) error {
			return j()
		})
	case func(context.Context):
		return FuncJob(func(ctx context.Context, _ *Cron, _ JobRun) error {
			j(ctx)
			return nil
		})
	case func(context.Context) error:
		return FuncJob(func(ctx context.Context, _ *Cron, _ JobRun) error { return j(ctx) })
	case func(EntryID):
		return FuncJob(func(_ context.Context, _ *Cron, r JobRun) error {
			j(r.Entry.ID)
			return nil
		})
	case func(EntryID) error:
		return FuncJob(func(_ context.Context, _ *Cron, r JobRun) error {
			return j(r.Entry.ID)
		})
	case func(Entry):
		return FuncJob(func(_ context.Context, _ *Cron, r JobRun) error {
			j(r.Entry)
			return nil
		})
	case func(Entry) error:
		return FuncJob(func(_ context.Context, _ *Cron, r JobRun) error {
			return j(r.Entry)
		})
	case func(*Cron):
		return FuncJob(func(_ context.Context, c *Cron, _ JobRun) error {
			j(c)
			return nil
		})
	case func(*Cron) error:
		return FuncJob(func(_ context.Context, c *Cron, _ JobRun) error {
			return j(c)
		})
	case func(context.Context, EntryID):
		return FuncJob(func(ctx context.Context, _ *Cron, r JobRun) error {
			j(ctx, r.Entry.ID)
			return nil
		})
	case func(context.Context, EntryID) error:
		return FuncJob(func(ctx context.Context, _ *Cron, r JobRun) error {
			return j(ctx, r.Entry.ID)
		})
	case func(context.Context, Entry):
		return FuncJob(func(ctx context.Context, _ *Cron, r JobRun) error {
			j(ctx, r.Entry)
			return nil
		})
	case func(context.Context, Entry) error:
		return FuncJob(func(ctx context.Context, _ *Cron, r JobRun) error {
			return j(ctx, r.Entry)
		})
	case func(context.Context, *Cron):
		return FuncJob(func(ctx context.Context, c *Cron, _ JobRun) error {
			j(ctx, c)
			return nil
		})
	case func(context.Context, *Cron) error:
		return FuncJob(func(ctx context.Context, c *Cron, r JobRun) error {
			return j(ctx, c)
		})
	case func(*Cron, EntryID):
		return FuncJob(func(_ context.Context, c *Cron, r JobRun) error {
			j(c, r.Entry.ID)
			return nil
		})
	case func(*Cron, EntryID) error:
		return FuncJob(func(_ context.Context, c *Cron, r JobRun) error {
			return j(c, r.Entry.ID)
		})
	case func(*Cron, Entry):
		return FuncJob(func(_ context.Context, c *Cron, r JobRun) error {
			j(c, r.Entry)
			return nil
		})
	case func(*Cron, Entry) error:
		return FuncJob(func(_ context.Context, c *Cron, r JobRun) error {
			return j(c, r.Entry)
		})
	case func(context.Context, *Cron, Entry):
		return FuncJob(func(ctx context.Context, c *Cron, r JobRun) error {
			j(ctx, c, r.Entry)
			return nil
		})
	case func(context.Context, *Cron, Entry) error:
		return FuncJob(func(ctx context.Context, c *Cron, r JobRun) error {
			return j(ctx, c, r.Entry)
		})
	case func(context.Context, *Cron, EntryID):
		return FuncJob(func(ctx context.Context, c *Cron, r JobRun) error {
			j(ctx, c, r.Entry.ID)
			return nil
		})
	case func(context.Context, *Cron, EntryID) error:
		return FuncJob(func(ctx context.Context, c *Cron, r JobRun) error {
			return j(ctx, c, r.Entry.ID)
		})
	case func(JobRun):
		return FuncJob(func(_ context.Context, _ *Cron, r JobRun) error {
			j(r)
			return nil
		})
	case func(JobRun) error:
		return FuncJob(func(_ context.Context, _ *Cron, r JobRun) error {
			return j(r)
		})
	case func(context.Context, JobRun):
		return FuncJob(func(ctx context.Context, _ *Cron, r JobRun) error {
			j(ctx, r)
			return nil
		})
	case func(context.Context, JobRun) error:
		return FuncJob(func(ctx context.Context, _ *Cron, r JobRun) error {
			return j(ctx, r)
		})
	case func(*Cron, JobRun):
		return FuncJob(func(_ context.Context, c *Cron, r JobRun) error {
			j(c, r)
			return nil
		})
	case func(*Cron, JobRun) error:
		return FuncJob(func(_ context.Context, c *Cron, r JobRun) error {
			return j(c, r)
		})
	case func(context.Context, *Cron, JobRun):
		return FuncJob(func(ctx context.Context, c *Cron, r JobRun) error {
			j(ctx, c, r)
			return nil
		})
	case func(context.Context, *Cron, JobRun) error:
		return FuncJob(func(ctx context.Context, c *Cron, r JobRun) error {
			return j(ctx, c, r)
		})
	case Job:
		return j
	case job1:
		return &job1Wrapper{j}
	case job2:
		return &job2Wrapper{j}
	case job3:
		return &job3Wrapper{j}
	case job4:
		return &job4Wrapper{j}
	case job5:
		return &job5Wrapper{j}
	case job6:
		return &job6Wrapper{j}
	case job7:
		return &job7Wrapper{j}
	case job8:
		return &job8Wrapper{j}
	case job9:
		return &job9Wrapper{j}
	case job10:
		return &job10Wrapper{j}
	case job11:
		return &job11Wrapper{j}
	case job12:
		return &job12Wrapper{j}
	case job13:
		return &job13Wrapper{j}
	case job14:
		return &job14Wrapper{j}
	case job15:
		return &job15Wrapper{j}
	case job16:
		return &job16Wrapper{j}
	case job17:
		return &job17Wrapper{j}
	case job18:
		return &job18Wrapper{j}
	case job19:
		return &job19Wrapper{j}
	case job20:
		return &job20Wrapper{j}
	case job21:
		return &job21Wrapper{j}
	case job22:
		return &job22Wrapper{j}
	case job23:
		return &job23Wrapper{j}
	case job24:
		return &job24Wrapper{j}
	case job25:
		return &job25Wrapper{j}
	case job26:
		return &job26Wrapper{j}
	case job27:
		return &job27Wrapper{j}
	case job28:
		return &job28Wrapper{j}
	case job29:
		return &job29Wrapper{j}
	case job30:
		return &job30Wrapper{j}
	case job31:
		return &job31Wrapper{j}
	default:
		panic(ErrUnsupportedJobType)
	}
}

type JobWrapper func(IntoJob) Job

// Once runs a job only one time, the entry is removed when the job starts.
func Once(job IntoJob) Job {
	inner := J(job) // Convert once, unsupported types panic when the wrapper is created
	return FuncJob(func(ctx context.Context, c *Cron, r JobRun) error {
		_ = c.removeKeepRun(r.Entry.ID, r.RunID) // Do not cancel the current run
		return inner.Run(ctx, c, r)
	})
}

// NWrapper creates a JobWrapper that runs a job "n" times, then removes the entry.
// It panics if n <= 0.
func NWrapper(n int) JobWrapper {
	if n <= 0 {
		panic("cron: N requires n > 0")
	}
	return func(job IntoJob) Job {
		var count atomic.Int32
		inner := J(job)
		return FuncJob(func(ctx context.Context, c *Cron, r JobRun) error {
			if newCount := count.Add(1); int(newCount) == n {
				_ = c.removeKeepRun(r.Entry.ID, r.RunID) // Do not cancel the current run
			}
			return inner.Run(ctx, c, r)
		})
	}
}

// N runs a job "n" times
func N(n int, j IntoJob) Job {
	return NWrapper(n)(j)
}

type ThresholdCallback func(ctx context.Context, c *Cron, r JobRun, threshold, dur time.Duration, err error)
type ThresholdCallbackAsync func(ctx context.Context, c *Cron, r JobRun, threshold time.Duration)

// ThresholdClbAsyncWrapper wraps a job and starts a timer for the given threshold.
// If the job runs longer than the threshold, the callback is triggered.
// The callback is invoked asynchronously and does not block job execution.
func ThresholdClbAsyncWrapper(threshold time.Duration, clb ThresholdCallbackAsync) JobWrapper {
	return func(job IntoJob) Job {
		inner := J(job)
		return FuncJob(func(ctx context.Context, c *Cron, r JobRun) error {
			done := make(chan struct{})
			timer := c.clock.NewTimer(threshold)
			defer func() {
				timer.Stop() // Stop synchronously, so the timer cannot fire after the job completed
				close(done)
			}()
			go func() {
				select {
				case <-timer.Chan(): // Job is still running after "threshold"
					clb(ctx, c, r, threshold)
				case <-done:
				}
			}()
			return inner.Run(ctx, c, r)
		})
	}
}

// ThresholdClbAsync calls the callback (asynchronously) if the job is still running after the threshold
func ThresholdClbAsync(threshold time.Duration, j IntoJob, clb ThresholdCallbackAsync) Job {
	return ThresholdClbAsyncWrapper(threshold, clb)(j)
}

func ThresholdClbWrapper(threshold time.Duration, clb ThresholdCallback) JobWrapper {
	return func(job IntoJob) Job {
		inner := J(job)
		return FuncJob(func(ctx context.Context, c *Cron, r JobRun) error {
			start := c.clock.Now()
			err := inner.Run(ctx, c, r)
			dur := c.clock.Since(start)
			if dur > threshold {
				clb(ctx, c, r, threshold, dur, err)
			}
			return err
		})
	}
}

// ThresholdClb execute a callback if the job runs longer than the specified threshold.
func ThresholdClb(threshold time.Duration, j IntoJob, clb ThresholdCallback) Job {
	return ThresholdClbWrapper(threshold, clb)(j)
}

// SkipIfStillRunning skips an invocation of the Job if a previous invocation is still running.
func SkipIfStillRunning(j IntoJob) Job {
	var running atomic.Bool
	inner := J(j)
	return FuncJob(func(ctx context.Context, c *Cron, r JobRun) (err error) {
		if running.CompareAndSwap(false, true) {
			defer running.Store(false)
			err = inner.Run(ctx, c, r)
		} else {
			return ErrJobAlreadyRunning
		}
		return
	})
}

// DelayIfStillRunning serializes jobs, delaying subsequent runs until the
// previous one is complete. Jobs running after a delay of more than a minute
// have the delay logged at Info.
func DelayIfStillRunning(j IntoJob) Job {
	var mu sync.Mutex
	inner := J(j)
	return FuncJob(func(ctx context.Context, c *Cron, r JobRun) (err error) {
		start := c.clock.Now()
		mu.Lock()
		defer mu.Unlock()
		if dur := c.clock.Since(start); dur > time.Minute {
			c.logger.Info("delay", "entryID", r.Entry.ID, "runID", r.RunID, "duration", dur)
		}
		return inner.Run(ctx, c, r)
	})
}

// JitterWrapper add some random delay before running the job
func JitterWrapper(duration time.Duration) JobWrapper {
	return func(j IntoJob) Job {
		inner := J(j)
		return FuncJob(func(ctx context.Context, c *Cron, r JobRun) error {
			delay := utils.RandDuration(0, max(duration, 0))
			select {
			case <-c.clock.After(delay):
			case <-ctx.Done():
				return ctx.Err()
			}
			return inner.Run(ctx, c, r)
		})
	}
}

// WithJitter add some random delay before running the job
func WithJitter(duration time.Duration, job IntoJob) Job {
	return JitterWrapper(duration)(job)
}

// TimeoutWrapper automatically cancel the job context after a given duration
func TimeoutWrapper(duration time.Duration) JobWrapper {
	return func(j IntoJob) Job {
		inner := J(j)
		return FuncJob(func(ctx context.Context, c *Cron, r JobRun) error {
			timeoutCtx, cancel := clockwork.WithTimeout(ctx, c.clock, duration)
			defer cancel()
			return inner.Run(timeoutCtx, c, r)
		})
	}
}

// WithTimeout cancels the context of the job after the given duration:
// `_, _ = cron.AddJob("* * * * * *", cron.WithTimeout(time.Second, func(ctx context.Context) { ... }))`
func WithTimeout(d time.Duration, job IntoJob) Job {
	return TimeoutWrapper(d)(job)
}

func DeadlineWrapper(deadline time.Time) JobWrapper {
	return func(j IntoJob) Job {
		inner := J(j)
		return FuncJob(func(ctx context.Context, c *Cron, r JobRun) error {
			deadlineCtx, cancel := clockwork.WithDeadline(ctx, c.clock, deadline)
			defer cancel()
			return inner.Run(deadlineCtx, c, r)
		})
	}
}

func WithDeadline(deadline time.Time, job IntoJob) Job {
	return DeadlineWrapper(deadline)(job)
}

// Chain `Chain(j, w1, w2, w3)` -> `w3(w2(w1(j)))`
func Chain(j IntoJob, wrappers ...JobWrapper) Job {
	job := J(j)
	for _, w := range wrappers {
		job = w(job)
	}
	return job
}

// WithRetryWrapper creates a JobWrapper that retries a job up to maxRetry times.
// It waits (attempt * 1s) between attempts, stops retrying when the context is cancelled,
// and returns the error of the last attempt.
func WithRetryWrapper(maxRetry int) JobWrapper {
	return func(job IntoJob) Job {
		inner := J(job)
		return FuncJob(func(ctx context.Context, c *Cron, r JobRun) error {
			for i := 0; ; i++ {
				err := inner.Run(ctx, c, r)
				if err == nil || i >= maxRetry {
					return err
				}
				select {
				case <-c.clock.After(time.Duration(i+1) * time.Second):
				case <-ctx.Done():
					return err
				}
			}
		})
	}
}

// WithRetry runs a job with retries up to maxRetry times
func WithRetry(maxRetry int, job IntoJob) Job {
	return WithRetryWrapper(maxRetry)(job)
}
