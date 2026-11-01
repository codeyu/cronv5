package cron

import (
	"sync"
	"sync/atomic"
)

// JobRunEventsBuffer is the number of job runs buffered by the channels of
// JobRunCreatedCh and JobRunCompletedCh, before new ones are dropped.
const JobRunEventsBuffer = 100

// jobRunEvents is a buffered channel of job runs, created when it is first requested:
// until then, no job run is exported for it.
type jobRunEvents struct {
	once sync.Once
	ch   atomic.Pointer[chan JobRun]
}

func (e *jobRunEvents) channel() <-chan JobRun {
	e.once.Do(func() {
		ch := make(chan JobRun, JobRunEventsBuffer)
		e.ch.Store(&ch)
	})
	return *e.ch.Load()
}

// send sends the job run to the channel if it was requested, without blocking: the job run
// is dropped if the channel is full.
func (e *jobRunEvents) send(c *Cron, jobRun *jobRunStruct) {
	ch := e.ch.Load()
	if ch == nil {
		return
	}
	if len(*ch) < cap(*ch) { // Do not export the run if it would be dropped
		select {
		case *ch <- jobRun.export():
			return
		default:
		}
	}
	c.logger.Debug("job run event dropped, the channel is full", "entryID", jobRun.entry.ID, "runID", jobRun.runID)
}
