# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **Month dependent day items** in cron specs (Quartz syntax):
  - day of month: `L` (last day), `L-n` (n days before the last day), `LW` (last weekday),
    `nW` (weekday nearest to the n-th);
  - day of week: `MON#2` / `1#2` (second Monday of the month), `5L` / `FRIL` (last Friday of the month).

  They can be listed with other items, eg: `0 0 1,15,L * *`. Specs using them were rejected before.
- **Day of week `7` is Sunday**, as in Vixie cron (`5-7` is Friday to Sunday).
- **Ranges wrapping around** in the month and day of week fields: `FRI-MON`, `NOV-FEB`, `FRI-TUE/2`.
  They were rejected before ("beginning of range beyond end of range").

### Changed

- `SpecSchedule` has a new unexported field. Code building a `SpecSchedule` with an unkeyed
  struct literal (`SpecSchedule{1, 1, ...}`) must use field names (`SpecSchedule{Second: 1, ...}`).

### Removed

- **The `webadmin` package was removed.** It was the only part of the module serving HTTP, and keeping it
  secure (XSS, CSRF, authentication) is out of scope for a scheduling library. The data it displayed is
  still available through the exported API (`Entries`, `RunningJobs`, `CompletedJobs`, hooks...), so an
  admin page can be built on top of it in the application.

## [v0.2.0] - 2026-10-30

This release fixes a number of concurrency bugs, adds graceful shutdown and memory limits,
and improves performance. The exported API only gained additions, existing code still compiles,
but several runtime behaviors changed: please read **Breaking changes** before upgrading.

### Breaking changes

Most impactful first.

- **Overdue entries are no longer run when the cron starts.** Entries added before `Start()` (or before a
  `Stop()`/`Start()`) whose next activation time is already in the past are rescheduled to their next
  activation time, instead of all running immediately. Entries using `RunOnStart`, `WithNext`, or `RunNow`
  called before `Start()` still run immediately.
- **`WithRetry` returns the error of the last attempt** (it used to always return `nil`). A job that fails
  every attempt now emits a `JobErr` event, triggers `JobErr` hooks, and logs an error.
- **Specs that never activate are rejected**: `AddJob`, `AddFunc`, `AddJobStrict` and `UpdateScheduleWithSpec`
  return `ErrUnsatisfiableSchedule` for specs such as `0 0 30 2 *` (February 30th). They used to be accepted
  and never run.
- **webadmin rejects cross-origin POST requests** with `403 Forbidden` (CSRF protection based on the
  `Sec-Fetch-Site` and `Origin` headers). Behind a reverse proxy that rewrites the `Host` header, make sure
  the `Origin` host matches the `Host` seen by the server.
- **Hooks receive the cron context instead of the job context.** The job context is now cancelled as soon as
  the job completes, so async `JobCompleted` hooks would otherwise get an already cancelled context.
  The hook context is still cancelled when the cron is stopped.
- **`Remove()` also removes the hooks registered for that entry** (`OnEntryEvt`, `OnEntryJobStart`, ...).
  Hooks must be registered again if an entry is re-added with the same ID.
- **`AddJob`, `Remove`, `UpdateSchedule`, `Enable`, `Disable`, `SetLocation`... no longer wait for the
  scheduler loop.** They return as soon as the entries are updated, the scheduler picks the change up
  right after. For example, a job added with `RunOnStart` to a running cron starts shortly after `AddJob`
  returns, instead of before. `RunNow` is unchanged: the job is started when it returns.
- **`Stop()` always returns a channel.** It used to return `nil` when the cron was not running (receiving
  from it blocked forever); it now returns a channel closed once the running jobs completed.
- **The job context is cancelled as soon as the job completes.** With `WithKeepCompletedRunsDur`, it used to
  stay alive until the run was cleaned up. Goroutines started by a job and still using its context are affected.
- **Wrappers panic when they are created with an unsupported job type** (`Once(1)`, `WithRetry(n, 1)`,
  `WithTimeout(d, 1)`...). The panic used to happen when the job ran, and was recovered as a `JobPanic`.
- **`N(n)` / `NWrapper(n)` panic when `n <= 0`.**
- **An empty time zone (`TZ= ...` / `CRON_TZ= ...`) is an error**, it was silently treated as UTC.
- **`Entry.Job()` only unwraps the library's own wrappers.** A user job struct with a single field used to be
  wrongly unwrapped, returning the value of that field instead of the job.
- **`ThresholdClbAsync` only calls the callback if the job is still running after the threshold.** It used to
  randomly miss slow jobs, and to report jobs that completed in time.
- Smaller changes:
  - `Schedule`, `AddEntry` return an error for a `nil` job or schedule (instead of panicking).
  - `SetCleanupInterval(0)` immediately frees the completed job runs that were kept.
  - The cleanup of completed job runs only runs while the cron is running.
  - webadmin escapes the job logs (they used to be rendered as raw HTML).
  - webadmin's completed page shows the latest 100 runs by default (`?limit=0` shows all of them).

### Added

- `Cron.Shutdown(ctx)`: graceful stop, waits for the running jobs without cancelling them; if `ctx` is done
  first, the jobs are cancelled and `ctx.Err()` is returned.
- `Builder.KeepJobsRunningOnStop()`: `Stop()` waits for the running jobs without cancelling their context.
- `Builder.WithMaxLogSize(n)`: keep at most the last `n` bytes of logs per job run.
- `Builder.WithMaxCompletedRunsPerEntry(n)`: keep at most `n` completed job runs per entry.
- `Cron.RecentCompletedJobs(limit)`: the most recent completed job runs, newest first, without exporting
  all of them.
- `ErrUnsatisfiableSchedule`.
- A stopped cron can be started again.
- Benchmarks (`bench_test.go`), runnable examples (`example_test.go`) and regression tests for every fix.
- GitHub Actions CI (gofmt, go vet, `go test -race` on Go 1.23 and 1.24), replacing Travis CI.

### Fixed

- `Once()` and `N()` cancelled the context of their own run before running the job.
- `Wait()` blocked forever when the cron was not running.
- `Start()` after `Stop()` did nothing (the scheduler loop exited immediately).
- A sync hook (`HookSync`) using the hooks API (`RemoveHook`, `OnEvt`...) deadlocked.
- Hooks of an entry could be run for another entry (`append` aliasing the global hooks slice), data race.
- Data race between a job writing its logs and the logs being read (webadmin, `RunningJobsFor`...).
- Exported `JobRun` snapshots shared memory with pooled job runs, and could be modified after being returned.
- `WithRetry` kept retrying after the context was cancelled, and waited after the last attempt.
- Removing an entry while it was running leaked its job runs and its hooks.
- Concurrent `AddJob` with the same ID (`WithID`) could all succeed, leaving the entries inconsistent.
- `RunningJobs`, `CompletedJobs`... could panic (nil pointer) when a job completed at the same time.
- Changing the cleanup interval could start several cleanup goroutines; a `CleanupNow` request could be
  lost when the cron was restarted.
- Stored XSS in webadmin's job run page (job logs rendered without escaping).
- `go vet` warning (copied `atomic.Pointer`).

### Changed

- `doc.go` rewritten: compiling examples, 5-field default format, stop/shutdown semantics, time zones.
- The `github.com/jonboulle/clockwork` fork `replace` was removed, the tests now use upstream clockwork
  v0.5.0 (the version downstream modules were already using).

### Performance

Measured on 4 cores, median of 3 runs (`go test -bench . -benchmem`):

| Benchmark                                | v0.1.0               | v0.2.0               | Speedup |
|------------------------------------------|----------------------|----------------------|---------|
| `UpdateSchedule` (running, 10k entries)  | 140 µs               | 0.22 µs              | 630×    |
| `RunNow` (running)                       | 121 µs               | 8.9 µs               | 13.6×   |
| Cleanup of completed runs (1k entries)   | 864 µs               | 64 µs                | 13.4×   |
| `Entries()` (10k entries)                | 12.7 ms              | 1.3 ms               | 9.7×    |
| `AddJob` (running)                       | 106 µs               | 18.8 µs              | 5.7×    |
| Parse with `CRON_TZ=`                    | 5.5 µs               | 1.2 µs               | 4.5×    |
| `CompletedJobs()` (1k runs)              | 2.5 ms, 2512 allocs  | 2.1 ms, 1512 allocs  | 1.2×    |

- The scheduler loop reuses a single timer.
- `Entries()` sorts a copy of the heap instead of popping a cloned heap.
- Job runs are created outside of the entries lock (`IDFactory` and `JobRunLoggerFactory` are user code).
- Locations from `CRON_TZ=`/`TZ=` are cached; the logs string is cached until the next write.
- webadmin templates are parsed once.

## [v0.1.0]

Initial release, fork of [robfig/cron](https://github.com/robfig/cron).

[v0.2.0]: https://github.com/codeyu/cronv5/compare/v0.1.0...v0.2.0
[v0.1.0]: https://github.com/codeyu/cronv5/releases/tag/v0.1.0
