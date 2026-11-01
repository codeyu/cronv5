package cron

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// Job runs are buffered until they are received, none is lost
func TestJobRunEventsBuffered(t *testing.T) {
	cron, _ := newRegressionCron()
	created, completed := cron.JobRunCreatedCh(), cron.JobRunCompletedCh()
	assert.Equal(t, created, cron.JobRunCreatedCh()) // the same channel for all the calls
	cron.Start()
	defer cron.Stop()
	id, _ := cron.AddJob("0 0 0 1 1 *", func() {})
	var runIDs []RunID
	for i := 0; i < 10; i++ {
		runID, err := cron.Trigger(id)
		assert.NoError(t, err)
		runIDs = append(runIDs, runID)
	}
	cron.waitAllJobsCompleted()
	collect := func(ch <-chan JobRun) (out []RunID) {
		for len(out) < len(runIDs) {
			select {
			case run := <-ch:
				out = append(out, run.RunID)
			case <-time.After(time.Second):
				t.Fatalf("received %d job runs out of %d", len(out), len(runIDs))
			}
		}
		return
	}
	assert.ElementsMatch(t, runIDs, collect(created))
	assert.ElementsMatch(t, runIDs, collect(completed))
}

// Nobody receives: the jobs are not blocked, the job runs over the buffer are dropped
func TestJobRunEventsFullBuffer(t *testing.T) {
	cron, _ := newRegressionCron()
	completed := cron.JobRunCompletedCh()
	cron.Start()
	defer cron.Stop()
	id, _ := cron.AddJob("0 0 0 1 1 *", func() {})
	first, _ := cron.Trigger(id)
	cron.waitAllJobsCompleted()
	for i := 0; i < JobRunEventsBuffer+50; i++ {
		_, _ = cron.Trigger(id)
	}
	done := make(chan struct{})
	go func() { cron.waitAllJobsCompleted(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("jobs blocked by a full channel")
	}
	assert.Len(t, completed, JobRunEventsBuffer)
	assert.Equal(t, first, (<-completed).RunID) // the oldest job runs are kept
}

// Until a channel is requested, no job run is sent (nor exported) for it
func TestJobRunEventsNotRequested(t *testing.T) {
	cron, _ := newRegressionCron()
	cron.Start()
	defer cron.Stop()
	id, _ := cron.AddJob("0 0 0 1 1 *", func() {})
	_, _ = cron.Trigger(id)
	cron.waitAllJobsCompleted()
	assert.Nil(t, cron.jobRunCreatedCh.ch.Load())
	assert.Nil(t, cron.jobRunCompletedCh.ch.Load())

	// Requested afterward: only the job runs from then on are received
	completed := cron.JobRunCompletedCh()
	assert.Empty(t, completed)
	runID, _ := cron.Trigger(id)
	cron.waitAllJobsCompleted()
	select {
	case run := <-completed:
		assert.Equal(t, runID, run.RunID)
	case <-time.After(time.Second):
		t.Fatal("no job run received")
	}
}
