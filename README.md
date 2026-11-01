# cronv5

[![Go Reference](https://pkg.go.dev/badge/github.com/codeyu/cronv5.svg)](https://pkg.go.dev/github.com/codeyu/cronv5)
[![CI](https://github.com/codeyu/cronv5/actions/workflows/ci.yml/badge.svg)](https://github.com/codeyu/cronv5/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

A cron scheduler for Go, built for running jobs in production: every run has its own context,
logger and history, and the scheduler handles daylight saving time, missed runs, concurrency
and retries for you.

It started as a fork of [robfig/cron](https://github.com/robfig/cron) and keeps its cron
expression format, plus the Quartz `L`, `W` and `#` extensions.

```go
c := cron.New().Build()
c.AddJob("0 9 * * MON-FRI", func(ctx context.Context) error {
	return sendReport(ctx) // cancelled on Stop, or when the run is cancelled
})
c.Start()
```

## Features

- **Cron expressions**: the standard 5 fields (an optional seconds field), `@daily`-style descriptors,
  `@every 1h30m`, Quartz `L` / `W` / `#` (`0 0 L * *`, `0 9 * * MON#1`), wrapping ranges (`FRI-MON`),
  `CRON_TZ=` time zones, including UTC offsets (`TZ=UTC+8`).
- **Correct around daylight saving time**: a job at a fixed time runs exactly once on transition days,
  like the Linux crontab (Vixie cron).
- **Jobs as plain functions**: `func()`, `func(context.Context) error`, `func(cron.JobRun)`... The panics
  are recovered, and each run gets a context, a `RunID` and its own logger.
- **Scheduling**: one-time jobs, run now (`Trigger`), misfire policies for runs missed while the
  machine was asleep or the cron was stopped, concurrency limits (global or per tag), retries with
  exponential backoff, timeouts, jitter.
- **Management**: string IDs, labels and tags, enable / disable / update / remove at runtime, next
  and previous activation times.
- **Observability**: hooks on job events, a metrics interface (Prometheus friendly), the history of the
  completed runs with their logs.
- **Graceful shutdown** and a **fake clock** for your tests.

## Installation

```bash
go get github.com/codeyu/cronv5
```

It requires Go 1.23 or newer. The package name is `cron`.

## Quick start

```go
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/codeyu/cronv5"
)

func main() {
	c := cron.New().Build()

	// Every 5 minutes
	_, _ = c.AddJob("*/5 * * * *", func() {
		log.Println("tick")
	})

	// Every weekday at 9:00 in Tokyo, with a timeout
	_, err := c.AddJob("CRON_TZ=Asia/Tokyo 0 9 * * MON-FRI", cron.WithTimeout(time.Minute, func(ctx context.Context) error {
		return nil // the context is cancelled after one minute
	}), cron.Label("daily report"))
	if err != nil {
		log.Fatal(err) // invalid spec, or a spec that never activates (eg: "0 0 30 2 *")
	}

	c.Start()

	// Wait for Ctrl+C, then let the running jobs finish (up to 30s)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := c.Shutdown(shutdownCtx); err != nil {
		log.Println("some jobs were cancelled:", err)
	}
}
```

## Cron expressions

### Format

```
┌───────────── minute (0-59)
│ ┌─────────── hour (0-23)
│ │ ┌───────── day of month (1-31)
│ │ │ ┌─────── month (1-12 or JAN-DEC)
│ │ │ │ ┌───── day of week (0-7 or SUN-SAT, 0 and 7 are Sunday)
│ │ │ │ │
* * * * *
```

With `cron.New().WithSeconds()`, a seconds field (0-59) comes first, and the day of week becomes optional.
Names are case insensitive (`MON`, `Mon`, `mon`).

| Character | Fields | Meaning | Example |
|---|---|---|---|
| `*` | all | every value | `* * * * *` every minute |
| `,` | all | list | `0 8,12,18 * * *` at 8:00, 12:00 and 18:00 |
| `-` | all | range | `0 9-17 * * *` every hour from 9:00 to 17:00 |
| `/` | all | step | `*/15 * * * *` every 15 minutes, `5/15` at 5, 20, 35, 50 |
| `?` | day of month, day of week | same as `*` | `0 0 ? * MON` |
| `L` | day of month | last day of the month, `L-n`: n days before it | `0 0 L * *`, `0 0 L-2 * *` |
| `W` | day of month | weekday (Mon-Fri) nearest to the day, `LW`: last weekday | `0 9 15W * *`, `0 18 LW * *` |
| `L` | day of week | last given day of the month | `0 9 * * 5L` (last Friday) |
| `#` | day of week | n-th given day of the month (1-5) | `0 9 * * MON#1` (first Monday) |

Ranges wrap around in the month and day of week fields: `FRI-MON` is Friday to Monday, `NOV-FEB`
is November to February. When both the day of month and day of week are restricted, the schedule
runs on days matching either one (as the crontab does): `0 0 1 * MON` runs on the 1st and on Mondays.

`AddJob` rejects the specs that never activate, such as `0 0 30 2 *` (February 30th), with `ErrUnsatisfiableSchedule`.

### Examples

| Spec | Runs |
|---|---|
| `0 * * * *` | every hour, on the hour |
| `30 2 * * *` | every day at 2:30 |
| `0 9 * * MON-FRI` | weekdays at 9:00 |
| `0 0 1 * *` | the first day of every month at midnight |
| `0 18 L * *` | the last day of every month at 18:00 |
| `0 18 LW * *` | the last weekday of every month at 18:00 |
| `0 10 * * MON#1` | the first Monday of every month at 10:00 |
| `0 10 * * FRI-SUN` | Friday, Saturday and Sunday at 10:00 |
| `0 0 1 NOV-FEB *` | the first day of November, December, January and February |
| `CRON_TZ=Europe/Paris 0 8 * * *` | every day at 8:00 in Paris |
| `TZ=UTC+8 0 8 * * *` | every day at 8:00 UTC+8 (China, Singapore...) |

### Descriptors and intervals

| Entry | Equivalent to |
|---|---|
| `@yearly` (`@annually`) | `0 0 1 1 *` |
| `@monthly` | `0 0 1 * *` |
| `@weekly` | `0 0 * * 0` |
| `@daily` (`@midnight`) | `0 0 * * *` |
| `@hourly` | `0 * * * *` |
| `@every <duration>` | every duration (`@every 1h30m`), starting when the job is added |

For `@every`, use `cron.RunOnStart` to also run the job right away, or `cron.WithNext(t)` to choose
the first run.

### Time zones and daylight saving time

The specs are interpreted in the location of the cron (`cron.New().WithLocation(loc)`, the machine's local
time zone by default), or the one given by a `CRON_TZ=` (or `TZ=`) prefix: an IANA name
(`CRON_TZ=America/New_York`) or a UTC offset (`TZ=UTC+8`, `TZ=UTC-05:30`, `TZ=+0900`). `UTC+8` is 8 hours
*ahead* of UTC, as in ISO 8601 (unlike the POSIX `TZ` variable and the IANA `Etc/GMT+8` zone).

Daylight saving time transitions are handled like Vixie cron:

| | Fixed time spec (`30 2 * * *`) | Interval spec (`*/5 * * * *`, `0 * * * *`) |
|---|---|---|
| Clock jumps forward (2:00 → 3:00) | runs once, at 3:00 | skips the times that do not exist |
| Clock goes back (2:00 → 1:00) | runs only the first time | runs both times |

A spec is an interval spec when its second, minute or hour field starts with `*`. Time zones without DST
(UTC, Asia/Tokyo, Asia/Shanghai...) are not affected. To embed the tz database in your program instead of
relying on the one of the machine, add `import _ "time/tzdata"`.

## Jobs

A job can be any of these functions, or a value with a `Run` method of one of these signatures, or a
`cron.Job`:

```go
func()
func() error
func(context.Context) error
func(cron.JobRun) error
func(context.Context, *cron.Cron, cron.JobRun) error
// ... and every combination of (context.Context, *cron.Cron, cron.EntryID | cron.Entry | cron.JobRun),
// with or without an error result: see the documentation of cron.J
```

Each run of a job is a `JobRun`, with a `RunID`, the `Entry`, its `ScheduledAt` time and its own logger:
what a job logs with `run.Logger()` is kept with the run (see [Job runs history](#job-runs-history)).
The context of a run is cancelled when the cron is stopped, when its entry is removed, with
`c.CancelJobRun(entryID, runID)`, and when the job completes. A job returning an error emits a
`JobErr` event, a panic is recovered and emits a `JobPanic` event.

```go
c.AddJob("0 * * * *", func(ctx context.Context, run cron.JobRun) error {
	run.Logger().Info("starting", "scheduledAt", run.ScheduledAt)
	return doWork(ctx)
}, cron.WithID("hourly-sync"), cron.Label("Sync the catalog"))
```

### Job wrappers

Wrappers add behavior to a job, and can be combined (`cron.Chain(job, w1, w2)` is `w2(w1(job))`):

| Wrapper | Behavior |
|---|---|
| `cron.WithTimeout(d, job)` | cancels the context after `d` |
| `cron.WithDeadline(t, job)` | cancels the context at `t` |
| `cron.WithRetry(n, job)` | retries up to `n` times (waits 1s, 2s, 3s...) |
| `cron.WithRetryPolicy(policy, job)` | retries with exponential backoff, jitter, and a `RetryIf` predicate |
| `cron.WithJitter(d, job)` | waits a random delay up to `d` before running |
| `cron.Once(job)` | runs once, then removes the entry |
| `cron.N(n, job)` | runs `n` times, then removes the entry |
| `cron.SkipIfStillRunning(job)` | skips a run while the previous one is running (returns `ErrJobAlreadyRunning`) |
| `cron.DelayIfStillRunning(job)` | waits for the previous run to complete |
| `cron.ThresholdClb(d, job, clb)` | calls `clb` if the job ran longer than `d` |
| `cron.ThresholdClbAsync(d, job, clb)` | calls `clb` as soon as the job runs longer than `d` |

Each one also exists as a `JobWrapper` (`TimeoutWrapper(d)`, `RetryPolicyWrapper(policy)`...), for `Chain`.

```go
job := cron.WithRetryPolicy(cron.RetryPolicy{
	MaxRetries:   5,
	InitialDelay: time.Second, // 1s, 2s, 4s, 8s, 16s
	MaxDelay:     time.Minute,
	Jitter:       0.2, // ±20%
	RetryIf:      func(err error) bool { return !errors.Is(err, errInvalidInput) },
}, callAPI)
c.AddJob("*/10 * * * *", cron.WithTimeout(30*time.Second, job))
```

## Managing entries

`AddJob` returns the `EntryID` of the entry. Options configure the entry when it is added:

| Option | Effect |
|---|---|
| `cron.WithID(id)` | a chosen ID instead of a generated one |
| `cron.Label(s)` | a description |
| `cron.WithTags(tags...)` | tags, to manage entries by group or limit their concurrency |
| `cron.RunOnStart` | also runs as soon as it is added (or the cron starts) |
| `cron.WithNext(t)` | the first activation time |
| `cron.Disabled` | added disabled |
| `cron.NoOverlap` | skips an activation while the previous run is still running |
| `cron.WithMisfirePolicy(p)` | what to do with missed activations (see below) |

```go
id, _ := c.AddJob("0 3 * * *", backup, cron.WithID("backup"), cron.WithTags("db", "nightly"))

runID, err := c.Trigger(id)           // run it now, its schedule is unchanged
err = c.Disable(id)                   // pause it, c.Enable(id) to resume
err = c.UpdateScheduleWithSpec(id, "0 4 * * *")
err = c.Remove(id)                    // cancels its running jobs too

c.DisableByTag("nightly")             // also EnableByTag, RemoveByTag, EntriesByTag
for _, entry := range c.Entries() {   // sorted by next activation time
	fmt.Println(entry.ID, entry.Label, entry.Next)
}
```

### One-time jobs

```go
// Runs once in 2 hours, then the entry is removed
c.AddJobAt(time.Now().Add(2*time.Hour), sendReminder, cron.Label("reminder"))
```

A time already past runs as soon as possible, including when the cron is started after it.

### Activation times

```go
next, _ := c.NextActivations(id, 5) // the next 5 runs of an entry, in the cron location

sched, _ := cron.ParseStandard("0 9 * * MON#1")
fmt.Println(cron.NextN(sched, time.Now(), 3))       // the next 3 activations
fmt.Println(sched.(*cron.SpecSchedule).Prev(time.Now())) // the previous one
for t := range cron.Activations(sched, time.Now()) { // all of them, until break
	if t.Year() > 2030 {
		break
	}
}
```

## Reliability

### Missed activations

An activation is missed when the cron was stopped at that time, or could not run it on time (eg: the
machine was asleep). The misfire policy, for the cron (`Builder.WithMisfirePolicy`) or an entry
(`cron.WithMisfirePolicy`), tells what to do:

| Policy | Missed while running | Missed while stopped |
|---|---|---|
| `MisfireDefault` | runs once | skipped |
| `MisfireSkip` | skipped (if later than the threshold) | skipped (if later than the threshold) |
| `MisfireRunOnce` | runs once | runs once |
| `MisfireRunAll` | each one runs (`JobRun.ScheduledAt` tells which) | each one runs |

The threshold of `MisfireSkip` is one minute (`Builder.WithMisfireThreshold`). Activations set explicitly
(`RunOnStart`, `WithNext`, `AddJobAt`) are never skipped.

```go
// Daily billing: if the server was down, catch up every missed day, one at a time
c.AddJob("0 1 * * *", cron.DelayIfStillRunning(bill), cron.WithMisfirePolicy(cron.MisfireRunAll))
```

### Concurrency

By default, a job runs at each activation, even if its previous run is still running.

```go
c := cron.New().
	WithMaxConcurrentJobs(10).           // at most 10 jobs at the same time
	WithTagConcurrencyLimit("db", 2).    // at most 2 jobs tagged "db"
	Build()
c.AddJob("*/5 * * * *", vacuum, cron.WithTags("db"), cron.NoOverlap) // and never twice at once
```

The runs over a limit wait for a slot (they can still be cancelled while waiting), the activations skipped by
`NoOverlap` are reported to the metrics.

### Stopping

| | Running jobs |
|---|---|
| `<-c.Stop()` | their context is cancelled, the channel is closed once they returned |
| `c.Shutdown(ctx)` | they can finish; if `ctx` is done first, they are cancelled and `ctx.Err()` is returned |
| `cron.New().KeepJobsRunningOnStop()` | `Stop` waits for them without cancelling them |

A stopped cron can be started again. `c.Run()` runs the scheduler in the current goroutine instead of `c.Start()`.

## Observability

### Hooks

Hooks are called on job events (`JobStart`, `JobCompleted`, `JobErr`, `JobPanic`), for all the entries or one of
them. They run in their own goroutine, unless `cron.HookSync` is given.

```go
c.OnEvt(cron.JobErr, func(ctx context.Context, c *cron.Cron, hookID cron.HookID, run cron.JobRun) {
	alert(run.Entry.Label, run.Error)
})
c.OnEntryJobCompleted(id, func(ctx context.Context, c *cron.Cron, hookID cron.HookID, run cron.JobRun) {
	log.Println(run.Entry.ID, "completed, error:", run.Error)
}, cron.HookLabel("log completions"))
```

### Metrics

Implement `cron.Metrics` (embed `cron.BaseMetrics` so that new methods do not break your type) and pass it to
`Builder.WithMetrics`. For example, with Prometheus:

```go
type promMetrics struct {
	cron.BaseMetrics
	runs     *prometheus.CounterVec   // labels: entry, result
	duration *prometheus.HistogramVec // labels: entry
	delay    *prometheus.HistogramVec // labels: entry
	skipped  *prometheus.CounterVec   // labels: entry, reason
}

func (m *promMetrics) JobStarted(run cron.JobRun, delay time.Duration) {
	m.delay.WithLabelValues(string(run.Entry.ID)).Observe(delay.Seconds())
}

func (m *promMetrics) JobCompleted(run cron.JobRun, duration time.Duration) {
	result := "ok"
	if run.Panic {
		result = "panic"
	} else if run.Error != nil {
		result = "error"
	}
	m.runs.WithLabelValues(string(run.Entry.ID), result).Inc()
	m.duration.WithLabelValues(string(run.Entry.ID)).Observe(duration.Seconds())
}

func (m *promMetrics) JobSkipped(entry cron.Entry, scheduledAt time.Time, reason cron.SkipReason) {
	m.skipped.WithLabelValues(string(entry.ID), reason.String()).Inc()
}
```

The methods are called synchronously, they must be fast. A panic in them is recovered and logged.

### Job runs history

```go
c := cron.New().
	WithKeepCompletedRunsDur(24 * time.Hour). // keep the completed runs for a day
	WithMaxCompletedRunsPerEntry(100).        // at most 100 per entry
	WithMaxLogSize(64 << 10).                 // at most 64 KiB of logs per run
	Build()

running := c.RunningJobs()
recent := c.RecentCompletedJobs(50)     // newest first
run, err := c.GetJobRun(entryID, runID) // with its Events, Error, Panic and Logs
```

The cron logs with `log/slog` by default (`Builder.WithLogger`, `Builder.Quiet()` to discard).

## Testing your jobs

The cron accepts a fake clock ([clockwork](https://github.com/jonboulle/clockwork)), so a test can run a schedule
of hours in milliseconds:

```go
func TestNightlyJob(t *testing.T) {
	clock := clockwork.NewFakeClockAt(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	c := cron.New().WithClock(clock).WithLocation(time.UTC).Quiet().Build()
	done := make(chan struct{})
	c.AddJob("0 3 * * *", func() { close(done) })
	c.Start()
	defer c.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := clock.BlockUntilContext(ctx, 1); err != nil { // the scheduler is waiting
		t.Fatal(err)
	}
	clock.Advance(3 * time.Hour)
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("the job did not run at 3:00")
	}
}
```

## Coming from robfig/cron

| | robfig/cron v3 | cronv5 |
|---|---|---|
| Create | `cron.New(cron.WithSeconds())` | `cron.New().WithSeconds().Build()` |
| Add a job | `c.AddFunc(spec, func())` | `c.AddJob(spec, func())` (and many other signatures) |
| Entry IDs | `int` | `string`, `cron.WithID` to choose it |
| Per run context, logger, history | no | yes |
| Panic recovery | opt-in (`cron.Recover`) | by default |
| `Stop()` | does not stop running jobs | cancels their context (`Shutdown(ctx)` for a graceful stop) |
| `L`, `W`, `#`, `7` as Sunday | no | yes |
| DST | skips or runs twice | runs once (Vixie cron) |
| Missed runs, concurrency limits, retries | no | yes |
| Hooks, metrics | no | yes |

The cron expression format and the `CRON_TZ=` prefix are compatible.

## Documentation

- [Package documentation](https://pkg.go.dev/github.com/codeyu/cronv5), including the full expression format
- [CHANGELOG](CHANGELOG.md), with the upgrade guide from v0.2.0

The API is stable since v1.0.0: the v1 releases only add features and fix bugs
([Semantic Versioning](https://semver.org/)).

## Contributing

```bash
go test -race ./...
```

Issues and pull requests are welcome. Please add a test with every fix.

## License

MIT, see [LICENSE](LICENSE). Originally based on [robfig/cron](https://github.com/robfig/cron),
copyright Rob Figueiredo.
