package cron

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// The first activation of "@every", as documented
func TestEveryFirstActivation(t *testing.T) {
	cron, clock := newRegressionCron()
	start := clock.Now()
	a, _ := cron.AddJob("@every 1h", func() {})
	b, _ := cron.AddJob("@every 1h", func() {}, RunOnStart)
	c, _ := cron.AddJob("@every 1h", func() {}, WithNext(start.Add(10*time.Minute)))

	next := func(id EntryID) time.Time {
		entry, err := cron.Entry(id)
		assert.NoError(t, err)
		return entry.Next
	}
	assert.Equal(t, start.Add(time.Hour), next(a)) // one interval after it was added

	clock.Advance(time.Minute)
	cron.Start()
	defer cron.Stop()
	cycle(cron)
	cron.waitAllJobsCompleted()
	assert.Equal(t, start.Add(time.Hour), next(a))             // unchanged by Start
	assert.Equal(t, start.Add(time.Minute+time.Hour), next(b)) // ran on start
	assert.Equal(t, start.Add(10*time.Minute), next(c))        // first activation chosen
	advanceAndCycle(cron, 9*time.Minute)
	assert.Equal(t, start.Add(10*time.Minute+time.Hour), next(c)) // then every hour

	// Added before Start, the first activation went by: one interval after the start
	cron2, clock2 := newRegressionCron()
	start2 := clock2.Now()
	d, _ := cron2.AddJob("@every 1h", func() {})
	clock2.Advance(2 * time.Hour)
	cron2.Start()
	defer cron2.Stop()
	cycle(cron2)
	entry, _ := cron2.Entry(d)
	assert.Equal(t, start2.Add(3*time.Hour), entry.Next)
}
