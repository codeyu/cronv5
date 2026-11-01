package cron

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestAtSchedule(t *testing.T) {
	at := time.Date(2026, 11, 2, 10, 0, 0, 0, time.UTC)
	s := At(at)
	assert.Equal(t, at, s.Next(at.Add(-time.Hour)))
	assert.True(t, s.Next(at).IsZero())
	assert.True(t, s.Next(at.Add(time.Hour)).IsZero())
	assert.Equal(t, []time.Time{at}, NextN(s, at.Add(-time.Hour), 3))

	tokyo, _ := time.LoadLocation("Asia/Tokyo")
	next := s.Next(at.Add(-time.Hour).In(tokyo))
	assert.Equal(t, tokyo, next.Location())
	assert.True(t, next.Equal(at))
}

func TestAddJobAt(t *testing.T) {
	cron, clock := newRegressionCron()
	cron.Start()
	defer cron.Stop()
	var calls atomic.Int32
	ctxErr := make(chan error, 1)
	at := clock.Now().Add(90 * time.Second)
	id, err := cron.AddJobAt(at, func(ctx context.Context) {
		calls.Add(1)
		ctxErr <- ctx.Err()
	})
	assert.NoError(t, err)

	entry, err := cron.Entry(id)
	assert.NoError(t, err)
	assert.Equal(t, at, entry.Next)
	next, err := cron.NextActivations(id, 3)
	assert.NoError(t, err)
	assert.Equal(t, []time.Time{at}, next)

	advanceAndCycle(cron, 89*time.Second)
	assert.Equal(t, int32(0), calls.Load())
	advanceAndCycle(cron, time.Second)
	assert.Equal(t, int32(1), calls.Load())
	assert.NoError(t, recvErrWithTimeout(t, ctxErr)) // the run is not cancelled by the removal

	_, err = cron.Entry(id)
	assert.ErrorIs(t, err, ErrEntryNotFound)
	advanceAndCycle(cron, time.Hour)
	assert.Equal(t, int32(1), calls.Load())
}

func TestAddJobAtPastTime(t *testing.T) {
	cron, clock := newRegressionCron()
	cron.Start()
	defer cron.Stop()
	var calls atomic.Int32
	id, err := cron.AddJobAt(clock.Now().Add(-time.Hour), func() { calls.Add(1) })
	assert.NoError(t, err)
	cycle(cron)
	cron.waitAllJobsCompleted()
	assert.Equal(t, int32(1), calls.Load())
	_, err = cron.Entry(id)
	assert.ErrorIs(t, err, ErrEntryNotFound)
}

func TestAddJobAtBeforeStart(t *testing.T) {
	cron, clock := newRegressionCron()
	var calls atomic.Int32
	_, err := cron.AddJobAt(clock.Now().Add(time.Minute), func() { calls.Add(1) })
	assert.NoError(t, err)
	// The time goes by while the cron is not running: the job runs when it starts.
	clock.Advance(time.Hour)
	cron.Start()
	defer cron.Stop()
	cycle(cron)
	cron.waitAllJobsCompleted()
	assert.Equal(t, int32(1), calls.Load())
	assert.Empty(t, cron.Entries())
}

func TestAddJobAtOptions(t *testing.T) {
	cron, clock := newRegressionCron()
	cron.Start()
	defer cron.Stop()
	var calls atomic.Int32
	at := clock.Now().Add(time.Minute)
	id, err := cron.AddJobAt(at, func() { calls.Add(1) }, WithID("reminder"), Label("send reminder"))
	assert.NoError(t, err)
	assert.Equal(t, EntryID("reminder"), id)
	entry, _ := cron.Entry(id)
	assert.Equal(t, "send reminder", entry.Label)

	// Disabled, then enabled before its time: it runs at its time.
	cron.Disable(id)
	cron.Enable(id)
	advanceAndCycle(cron, time.Minute)
	assert.Equal(t, int32(1), calls.Load())

	_, err = cron.AddJobAt(time.Time{}, func() {})
	assert.Error(t, err)
}
