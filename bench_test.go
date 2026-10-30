package cron

import (
	"fmt"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
)

func newBenchCron(b *testing.B, opts ...func(*Builder)) (*Cron, *clockwork.FakeClock) {
	b.Helper()
	clock := clockwork.NewFakeClockAt(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC))
	builder := New().WithClock(clock).WithParser(secondParser).Quiet()
	for _, opt := range opts {
		opt(builder)
	}
	return builder.Build(), clock
}

// AddJob while the cron is running (each add notifies the scheduler loop)
func BenchmarkAddJobRunning(b *testing.B) {
	cron, _ := newBenchCron(b)
	cron.Start()
	defer cron.Stop()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = cron.AddJob("0 0 0 1 1 *", func() {})
	}
}

func BenchmarkEntries(b *testing.B) {
	for _, n := range []int{100, 10_000} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			cron, _ := newBenchCron(b)
			for i := 0; i < n; i++ {
				_, _ = cron.Schedule(Every(time.Duration(i+1)*time.Second), J(func() {}))
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = cron.Entries()
			}
		})
	}
}

func BenchmarkParse(b *testing.B) {
	for _, spec := range []string{"*/5 1-10 * * MON-FRI", "CRON_TZ=Asia/Tokyo 0 6 * * *", "@every 1h30m"} {
		b.Run(spec, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, _ = ParseStandard(spec)
			}
		})
	}
}

func BenchmarkSpecNext(b *testing.B) {
	sched, _ := ParseStandard("0 6 1 * MON")
	t := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		t = sched.Next(t)
	}
}

// One scheduler tick running 1000 due jobs
func BenchmarkDispatch1000(b *testing.B) {
	cron, _ := newBenchCron(b)
	for i := 0; i < 1000; i++ {
		_, _ = cron.AddJob("* * * * * *", func() {})
	}
	cron.Start()
	defer cron.Stop()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		advanceAndCycle(cron, time.Second)
	}
}

func BenchmarkCompletedJobs(b *testing.B) {
	cron, _ := newBenchCron(b, func(b *Builder) { b.WithKeepCompletedRunsDur(time.Hour) })
	for i := 0; i < 100; i++ {
		_, _ = cron.AddJob("* * * * * *", func(jr JobRun) { jr.Logger().Info("some log line") })
	}
	cron.Start()
	defer cron.Stop()
	for i := 0; i < 10; i++ {
		advanceAndCycle(cron, time.Second)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = cron.CompletedJobs()
	}
}

func BenchmarkCleanupOldRuns(b *testing.B) {
	cron, _ := newBenchCron(b, func(b *Builder) { b.WithKeepCompletedRunsDur(time.Hour) })
	for i := 0; i < 1000; i++ {
		_, _ = cron.AddJob("* * * * * *", func() {})
	}
	cron.Start()
	defer cron.Stop()
	advanceAndCycle(cron, time.Second)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cron.cleanupOldRuns(cron.ctx(), time.Hour) // Nothing to clean, measures the traversal
	}
}
