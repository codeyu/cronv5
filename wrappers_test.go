package cron

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// captureLogger records the messages logged
type captureLogger struct {
	mu   sync.Mutex
	msgs []string
}

func (l *captureLogger) log(level, msg string, kv ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.msgs = append(l.msgs, fmt.Sprint(level, " ", msg, " ", kv))
}
func (l *captureLogger) Debug(msg string, kv ...any) { l.log("DEBUG", msg, kv...) }
func (l *captureLogger) Info(msg string, kv ...any)  { l.log("INFO", msg, kv...) }
func (l *captureLogger) Warn(msg string, kv ...any)  { l.log("WARN", msg, kv...) }
func (l *captureLogger) Error(msg string, kv ...any) { l.log("ERROR", msg, kv...) }

func (l *captureLogger) contains(s string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, msg := range l.msgs {
		if strings.Contains(msg, s) {
			return true
		}
	}
	return false
}

func TestChainOrder(t *testing.T) {
	var order []string
	wrapper := func(name string) JobWrapper {
		return func(j IntoJob) Job {
			inner := J(j)
			return FuncJob(func(ctx context.Context, c *Cron, r JobRun) error {
				order = append(order, name)
				return inner.Run(ctx, c, r)
			})
		}
	}
	job := Chain(func() { order = append(order, "job") }, wrapper("w1"), wrapper("w2"), wrapper("w3"))
	assert.NoError(t, job.Run(context.Background(), nil, JobRun{}))
	// Chain(j, w1, w2, w3) is w3(w2(w1(j))): the last wrapper runs first
	assert.Equal(t, []string{"w3", "w2", "w1", "job"}, order)

	assert.NoError(t, Chain(func() {}).Run(context.Background(), nil, JobRun{}))
}

func TestDelayIfStillRunning(t *testing.T) {
	logger := &captureLogger{}
	cron, clock := newRegressionCron(func(b *Builder) { b.WithLogger(logger) })
	release := make(chan struct{})
	var running, maxRunning, calls atomic.Int32
	job := DelayIfStillRunning(func() {
		n := running.Add(1)
		if n > maxRunning.Load() {
			maxRunning.Store(n)
		}
		if calls.Add(1) == 1 {
			<-release
		}
		running.Add(-1)
	})
	first := make(chan error, 1)
	go func() { first <- job.Run(context.Background(), cron, JobRun{}) }()
	assert.Eventually(t, func() bool { return calls.Load() == 1 }, time.Second, time.Millisecond)
	second := make(chan error, 1)
	go func() { second <- job.Run(context.Background(), cron, JobRun{Entry: Entry{ID: "delayed"}}) }()
	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, int32(1), calls.Load()) // delayed, not skipped
	clock.Advance(2 * time.Minute)          // the second run waits for more than a minute
	close(release)
	assert.NoError(t, recvErrWithTimeout(t, first))
	assert.NoError(t, recvErrWithTimeout(t, second))
	assert.Equal(t, int32(2), calls.Load())
	assert.Equal(t, int32(1), maxRunning.Load())
	assert.True(t, logger.contains("INFO delay"))
}

func TestJitter(t *testing.T) {
	cron, clock := newRegressionCron()
	var ran atomic.Bool
	job := WithJitter(time.Minute, func() { ran.Store(true) })
	done := make(chan error, 1)
	go func() { done <- job.Run(context.Background(), cron, JobRun{}) }()
	clock.BlockUntil(1) // waiting for a random delay up to a minute
	clock.Advance(time.Minute)
	assert.NoError(t, recvErrWithTimeout(t, done))
	assert.True(t, ran.Load())

	// Cancelled while waiting: the job does not run
	ran.Store(false)
	ctx, cancel := context.WithCancel(context.Background())
	go func() { done <- JitterWrapper(time.Hour)(func() { ran.Store(true) }).Run(ctx, cron, JobRun{}) }()
	clock.BlockUntil(1)
	cancel()
	assert.ErrorIs(t, recvErrWithTimeout(t, done), context.Canceled)
	assert.False(t, ran.Load())

	// No jitter (zero or negative duration): runs right away
	for _, d := range []time.Duration{0, -time.Second} {
		ran.Store(false)
		go func() { done <- WithJitter(d, func() { ran.Store(true) }).Run(context.Background(), cron, JobRun{}) }()
		assert.NoError(t, recvErrWithTimeout(t, done))
		assert.True(t, ran.Load())
	}
}

func TestThresholdClb(t *testing.T) {
	cron, clock := newRegressionCron()
	type call struct {
		threshold, dur time.Duration
		err            error
	}
	calls := make(chan call, 1)
	clb := func(_ context.Context, _ *Cron, _ JobRun, threshold, dur time.Duration, err error) {
		calls <- call{threshold, dur, err}
	}
	errSlow := errors.New("slow")
	slow := ThresholdClb(time.Minute, func() error { clock.Advance(2 * time.Minute); return errSlow }, clb)
	assert.ErrorIs(t, slow.Run(context.Background(), cron, JobRun{}), errSlow)
	select {
	case c := <-calls:
		assert.Equal(t, call{time.Minute, 2 * time.Minute, errSlow}, c)
	default:
		t.Fatal("callback not called for a slow job")
	}

	fast := ThresholdClbWrapper(time.Minute, clb)(func() { clock.Advance(time.Minute) }) // not longer than the threshold
	assert.NoError(t, fast.Run(context.Background(), cron, JobRun{}))
	select {
	case <-calls:
		t.Fatal("callback called for a fast job")
	default:
	}
}

func TestWithSeconds(t *testing.T) {
	cron := New().WithSeconds().WithLogger(newNoOpLogger()).Build()
	_, err := cron.AddJob("*/10 * * * * *", func() {})
	assert.NoError(t, err)
	_, err = cron.AddJob("* * * * *", func() {}) // the day of week is optional, 5 fields are accepted
	assert.NoError(t, err)
	_, err = cron.AddJob("* * * *", func() {})
	assert.Error(t, err)
}

func TestBaseMetricsDoNothing(t *testing.T) {
	var m Metrics = BaseMetrics{}
	m.JobStarted(JobRun{}, 0)
	m.JobCompleted(JobRun{}, 0)
	m.JobSkipped(Entry{}, time.Time{}, SkipMisfire)
}

func TestWithOptionalSeconds(t *testing.T) {
	from := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	parse := func(b *Builder, spec string) ([]time.Time, error) {
		c := b.WithLogger(newNoOpLogger()).Build()
		sched, err := c.parser.Parse(spec)
		if err != nil {
			return nil, err
		}
		return NextN(sched, from, 2), nil
	}
	at := func(h, m, s int) []time.Time {
		return []time.Time{time.Date(2026, 1, 1, h, m, s, 0, time.UTC), time.Date(2026, 1, 2, h, m, s, 0, time.UTC)}
	}

	// 5 fields: no seconds, as by default
	got, err := parse(New().WithOptionalSeconds(), "TZ=UTC 0 9 * * *")
	assert.NoError(t, err)
	assert.Equal(t, at(9, 0, 0), got)
	// 6 fields: seconds first
	got, err = parse(New().WithOptionalSeconds(), "TZ=UTC 30 0 9 * * *")
	assert.NoError(t, err)
	assert.Equal(t, at(9, 0, 30), got)
	// Descriptors
	got, err = parse(New().WithOptionalSeconds(), "TZ=UTC @every 10s")
	assert.NoError(t, err)
	assert.Equal(t, []time.Time{from.Add(10 * time.Second), from.Add(20 * time.Second)}, got)
	// Too few or too many fields
	for _, spec := range []string{"* * * *", "* * * * * * *"} {
		_, err = parse(New().WithOptionalSeconds(), spec)
		assert.Error(t, err, spec)
	}

	// The pitfall of WithSeconds: a 5 fields spec has seconds first
	got, err = parse(New().WithSeconds(), "TZ=UTC 0 9 * * *")
	assert.NoError(t, err)
	assert.Equal(t, []time.Time{time.Date(2026, 1, 1, 0, 9, 0, 0, time.UTC), time.Date(2026, 1, 1, 1, 9, 0, 0, time.UTC)}, got)
	// And the default rejects 6 fields
	_, err = parse(New(), "TZ=UTC 30 0 9 * * *")
	assert.Error(t, err)
}
