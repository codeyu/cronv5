package cron

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRetryPolicyDelay(t *testing.T) {
	tests := []struct {
		policy   RetryPolicy
		expected []time.Duration
	}{
		{RetryPolicy{}, []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second}},
		{RetryPolicy{InitialDelay: 100 * time.Millisecond, Multiplier: 3}, []time.Duration{100 * time.Millisecond, 300 * time.Millisecond, 900 * time.Millisecond}},
		{RetryPolicy{Multiplier: 1}, []time.Duration{time.Second, time.Second, time.Second}},
		{RetryPolicy{Multiplier: 0.5}, []time.Duration{time.Second, time.Second}}, // below 1: constant
		{RetryPolicy{MaxDelay: 3 * time.Second}, []time.Duration{time.Second, 2 * time.Second, 3 * time.Second, 3 * time.Second}},
		{RetryPolicy{InitialDelay: -time.Second}, []time.Duration{time.Second}},
	}
	for _, tt := range tests {
		for n, expected := range tt.expected {
			assert.Equal(t, expected, tt.policy.Delay(n), "%+v retry %d", tt.policy, n)
		}
	}
	assert.Equal(t, time.Second, RetryPolicy{}.Delay(-1))
	// No overflow for a large number of retries
	assert.Equal(t, time.Duration(math.MaxInt64), RetryPolicy{}.Delay(1000))
	assert.Equal(t, time.Hour, RetryPolicy{MaxDelay: time.Hour}.Delay(1000))
}

func TestRetryPolicyJitter(t *testing.T) {
	policy := RetryPolicy{InitialDelay: 10 * time.Second, Multiplier: 1, Jitter: 0.2}
	seen := map[time.Duration]bool{}
	for i := 0; i < 1000; i++ {
		d := policy.Delay(3)
		assert.GreaterOrEqual(t, d, 8*time.Second)
		assert.LessOrEqual(t, d, 12*time.Second)
		seen[d] = true
	}
	assert.Greater(t, len(seen), 100) // randomized
	// Jitter is capped at 1
	for i := 0; i < 100; i++ {
		d := RetryPolicy{Jitter: 5}.Delay(0)
		assert.GreaterOrEqual(t, d, time.Duration(0))
		assert.LessOrEqual(t, d, 2*time.Second)
	}
}

// waitRetry waits until the job waits before a retry, checks it does not retry before "delay", then advances the clock
func waitRetry(t *testing.T, cron *Cron, attempts *atomic.Int32, delay time.Duration) {
	t.Helper()
	clock := cron.clock.(interface {
		BlockUntil(int)
		Advance(time.Duration)
	})
	clock.BlockUntil(1)
	before := attempts.Load()
	clock.Advance(delay - time.Nanosecond)
	assert.Equal(t, before, attempts.Load(), "retried before %v", delay)
	clock.Advance(time.Nanosecond)
}

func TestWithRetryPolicy(t *testing.T) {
	cron, _ := newRegressionCron()
	var attempts atomic.Int32
	job := WithRetryPolicy(RetryPolicy{MaxRetries: 4, MaxDelay: 3 * time.Second}, func() error {
		if n := attempts.Add(1); n < 4 {
			return fmt.Errorf("err%d", n)
		}
		return nil
	})
	ch := make(chan error, 1)
	go func() { ch <- job.Run(context.Background(), cron, JobRun{}) }()
	waitRetry(t, cron, &attempts, time.Second)
	waitRetry(t, cron, &attempts, 2*time.Second)
	waitRetry(t, cron, &attempts, 3*time.Second) // capped
	assert.NoError(t, recvErrWithTimeout(t, ch))
	assert.Equal(t, int32(4), attempts.Load())
}

func TestWithRetryPolicyReturnsLastError(t *testing.T) {
	cron, _ := newRegressionCron()
	var attempts atomic.Int32
	job := WithRetryPolicy(RetryPolicy{MaxRetries: 2, Multiplier: 1}, func() error { return fmt.Errorf("err%d", attempts.Add(1)) })
	ch := make(chan error, 1)
	go func() { ch <- job.Run(context.Background(), cron, JobRun{}) }()
	waitRetry(t, cron, &attempts, time.Second)
	waitRetry(t, cron, &attempts, time.Second)
	assert.EqualError(t, recvErrWithTimeout(t, ch), "err3")
	assert.Equal(t, int32(3), attempts.Load())
}

var errPermanent = errors.New("permanent")

func TestWithRetryPolicyRetryIf(t *testing.T) {
	cron, _ := newRegressionCron()
	var attempts atomic.Int32
	job := WithRetryPolicy(RetryPolicy{MaxRetries: 5, RetryIf: func(err error) bool { return !errors.Is(err, errPermanent) }}, func() error {
		if attempts.Add(1) == 1 {
			return errors.New("temporary")
		}
		return fmt.Errorf("wrapped: %w", errPermanent)
	})
	ch := make(chan error, 1)
	go func() { ch <- job.Run(context.Background(), cron, JobRun{}) }()
	waitRetry(t, cron, &attempts, time.Second)
	assert.ErrorIs(t, recvErrWithTimeout(t, ch), errPermanent) // not retried
	assert.Equal(t, int32(2), attempts.Load())
}

func TestWithRetryPolicyCancelled(t *testing.T) {
	cron, clock := newRegressionCron()
	var attempts atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	job := WithRetryPolicy(RetryPolicy{MaxRetries: 10}, func() error { attempts.Add(1); return errors.New("boom") })
	ch := make(chan error, 1)
	go func() { ch <- job.Run(ctx, cron, JobRun{}) }()
	clock.BlockUntil(1)
	cancel()
	assert.EqualError(t, recvErrWithTimeout(t, ch), "boom")
	assert.Equal(t, int32(1), attempts.Load())

	// Cancelled during an attempt: no retry
	attempts.Store(0)
	ctx, cancel = context.WithCancel(context.Background())
	job = WithRetryPolicy(RetryPolicy{MaxRetries: 10}, func() error { attempts.Add(1); cancel(); return errors.New("boom") })
	assert.EqualError(t, job.Run(ctx, cron, JobRun{}), "boom")
	assert.Equal(t, int32(1), attempts.Load())
}

// In a cron, the retries are logged in the logs of the job run, and the final error is the run error
func TestWithRetryPolicyInCron(t *testing.T) {
	cron, clock := newRegressionCron(func(b *Builder) { b.WithKeepCompletedRunsDur(time.Hour) })
	cron.Start()
	defer cron.Stop()
	var attempts atomic.Int32
	id, _ := cron.AddJob("0 0 0 1 1 *", WithRetryPolicy(RetryPolicy{MaxRetries: 1}, func() error {
		return fmt.Errorf("err%d", attempts.Add(1))
	}))
	runID, err := cron.Trigger(id)
	assert.NoError(t, err)
	// Advance the clock until the job is retried
	assert.Eventually(t, func() bool {
		clock.Advance(time.Second)
		return attempts.Load() == 2
	}, 5*time.Second, time.Millisecond)
	cron.waitAllJobsCompleted()
	run, err := cron.GetJobRun(id, runID)
	assert.NoError(t, err)
	assert.EqualError(t, run.Error, "err2")
	assert.Contains(t, run.Logs, "job failed, retrying")
	assert.Contains(t, run.Logs, "err1")
}
