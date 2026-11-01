package cron

import (
	"context"
	"slices"
)

// WithMaxConcurrentJobs limits the number of jobs running at the same time (0 = unlimited).
// The runs over the limit wait for a running job to complete (they are registered as running,
// see IsRunning), unless their context is cancelled (Stop, Remove, CancelJobRun...) while waiting.
func (b *Builder) WithMaxConcurrentJobs(n int) *Builder {
	b.maxConcurrentJobs = max(n, 0)
	return b
}

// WithTagConcurrencyLimit limits the number of jobs of the entries having the tag (see WithTags)
// running at the same time (0 = unlimited). It can be set for several tags, a run of an entry having
// several limited tags waits until it is under all the limits. Runs over the limit wait, as with
// WithMaxConcurrentJobs.
func (b *Builder) WithTagConcurrencyLimit(tag string, n int) *Builder {
	if b.tagLimits == nil {
		b.tagLimits = make(map[string]int)
	}
	b.tagLimits[tag] = max(n, 0)
	return b
}

// semaphore limits the concurrency, a slot is acquired by sending to the channel
type semaphore chan struct{}

// concurrencyLimits holds the semaphores of the concurrency limits, it is not modified after Build
type concurrencyLimits struct {
	global semaphore            // nil = unlimited
	tags   map[string]semaphore // only the limited tags
}

func newConcurrencyLimits(maxConcurrentJobs int, tagLimits map[string]int) concurrencyLimits {
	var limits concurrencyLimits
	if maxConcurrentJobs > 0 {
		limits.global = make(semaphore, maxConcurrentJobs)
	}
	for tag, n := range tagLimits {
		if n > 0 {
			if limits.tags == nil {
				limits.tags = make(map[string]semaphore)
			}
			limits.tags[tag] = make(semaphore, n)
		}
	}
	return limits
}

// acquire waits for a slot of each limit applying to a run of an entry having the given tags.
// The slots are always acquired in the same order (global, then tags sorted), so that runs
// waiting for several limits cannot deadlock. It returns the context error if it is cancelled
// while waiting, without holding any slot.
func (l concurrencyLimits) acquire(ctx context.Context, tags []string) (release func(), err error) {
	var sems []semaphore
	if l.global != nil {
		sems = append(sems, l.global)
	}
	if len(l.tags) > 0 {
		sorted := slices.Clone(tags)
		slices.Sort(sorted)
		for _, tag := range slices.Compact(sorted) {
			if sem, ok := l.tags[tag]; ok {
				sems = append(sems, sem)
			}
		}
	}
	release = func() {
		for i := len(sems) - 1; i >= 0; i-- {
			<-sems[i]
		}
	}
	for i, sem := range sems {
		// Checked first: select picks randomly when a slot is free and the context is cancelled
		if err := ctx.Err(); err != nil {
			sems = sems[:i] // release the acquired slots only
			release()
			return nil, err
		}
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			sems = sems[:i]
			release()
			return nil, ctx.Err()
		}
	}
	if err := ctx.Err(); len(sems) > 0 && err != nil { // Cancelled while the last slot was acquired
		release()
		return nil, err
	}
	return release, nil
}
