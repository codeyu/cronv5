package cron

import (
	"context"
	"math"
	"math/rand/v2"
	"time"
)

// RetryPolicy configures the retries of WithRetryPolicy / RetryPolicyWrapper.
//
// The delay before the retry n (starting at 0) is InitialDelay * Multiplier^n, capped at MaxDelay,
// then randomized by ±Jitter: with the defaults, the retries wait 1s, 2s, 4s, 8s...
type RetryPolicy struct {
	// MaxRetries is the maximum number of retries after the first attempt.
	MaxRetries int
	// InitialDelay is the delay before the first retry (default: one second).
	InitialDelay time.Duration
	// MaxDelay caps the delay between two attempts (0 = no cap).
	MaxDelay time.Duration
	// Multiplier is the growth of the delay between two retries (default: 2, 1 = constant delay).
	// Values below 1 are treated as 1.
	Multiplier float64
	// Jitter randomizes each delay by ±Jitter, from 0 (default, no randomization) to 1:
	// 0.2 is a delay between 80% and 120% of the computed one. Spreads the retries of many jobs.
	Jitter float64
	// RetryIf tells if an error should be retried (nil = all errors are retried).
	RetryIf func(error) bool
}

// Delay returns the delay before the retry n (starting at 0), see RetryPolicy.
func (p RetryPolicy) Delay(n int) time.Duration {
	initial := p.InitialDelay
	if initial <= 0 {
		initial = time.Second
	}
	multiplier := p.Multiplier
	if multiplier == 0 {
		multiplier = 2
	}
	multiplier = max(multiplier, 1)
	delay := float64(initial) * math.Pow(multiplier, float64(max(n, 0)))
	if p.MaxDelay > 0 {
		delay = min(delay, float64(p.MaxDelay))
	}
	if jitter := min(max(p.Jitter, 0), 1); jitter > 0 {
		delay *= 1 + jitter*(2*rand.Float64()-1)
	}
	// float64(math.MaxInt64) is rounded up to 2^63, which would overflow the conversion
	if delay >= float64(math.MaxInt64) {
		return time.Duration(math.MaxInt64)
	}
	return time.Duration(delay)
}

// RetryPolicyWrapper creates a JobWrapper retrying the job when it returns an error, as configured
// by the policy. It stops retrying when the context is cancelled, and returns the error of the last
// attempt. The retries are logged with the logger of the job run.
func RetryPolicyWrapper(policy RetryPolicy) JobWrapper {
	return func(job IntoJob) Job {
		inner := J(job)
		return FuncJob(func(ctx context.Context, c *Cron, r JobRun) error {
			for retry := 0; ; retry++ {
				err := inner.Run(ctx, c, r)
				if err == nil || retry >= policy.MaxRetries || ctx.Err() != nil ||
					(policy.RetryIf != nil && !policy.RetryIf(err)) {
					return err
				}
				delay := policy.Delay(retry)
				if logger := r.Logger(); logger != nil {
					logger.Warn("job failed, retrying", "retry", retry+1, "maxRetries", policy.MaxRetries, "delay", delay, "error", err)
				}
				select {
				case <-c.clock.After(delay):
				case <-ctx.Done():
					return err
				}
			}
		})
	}
}

// WithRetryPolicy runs a job with retries, as configured by the policy (see RetryPolicy):
//
//	cron.WithRetryPolicy(cron.RetryPolicy{MaxRetries: 5, MaxDelay: time.Minute, Jitter: 0.2}, job)
func WithRetryPolicy(policy RetryPolicy, job IntoJob) Job {
	return RetryPolicyWrapper(policy)(job)
}
