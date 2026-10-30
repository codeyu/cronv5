package cron

import (
	"bytes"
	"context"
	"slices"
	"sync"
	"time"

	"github.com/codeyu/cronv5/internal/mtx"
	"github.com/jonboulle/clockwork"
)

// RunID represents a unique identifier for a job run.
type RunID ID

// JobRun represents a single execution of a scheduled job.
type JobRun struct {
	RunID       RunID      // Unique identifier for this job run
	Entry       Entry      // Job definition being executed
	CreatedAt   time.Time  // When the run was created
	StartedAt   *time.Time // When execution began (nil if not started)
	CompletedAt *time.Time // When execution finished (nil if not completed)
	Events      []JobEvent // Timeline of events during execution
	Error       error      // Final error state if any
	Panic       bool       // Whether job panicked during execution
	Logs        string     // Captured log output
	logger      Logger     // Internal logger instance
}

// Logger returns the structured logger for this job run
func (j JobRun) Logger() Logger {
	return j.logger
}

type jobRunStruct struct {
	runID     RunID
	entry     Entry
	clock     clockwork.Clock
	inner     mtx.RWMtx[jobRunInner]
	createdAt time.Time
	cronCtx   context.Context // Context of the cron when the run was created
	ctx       context.Context // Context of the run, cancelled when the job completes
	cancel    context.CancelFunc
	logsBuf   *syncBuffer
	logger    Logger
}

// syncBuffer is a bytes.Buffer safe for concurrent use,
// the job writes its logs while they can be read by anyone inspecting the run.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

type jobRunInner struct {
	startedAt   *time.Time
	completedAt *time.Time
	events      []JobEvent
	error       error
	panic       bool
}

func newJobRun(ctx context.Context, clock clockwork.Clock, entry Entry, idFactory IDFactory, loggerFactory JobRunLoggerFactory) *jobRunStruct {
	jr := &jobRunStruct{
		runID:     RunID(idFactory.Next()),
		entry:     entry,
		clock:     clock,
		createdAt: clock.Now(),
		cronCtx:   ctx,
		logsBuf:   &syncBuffer{},
		inner: mtx.NewRWMtx(jobRunInner{
			events: make([]JobEvent, 0, 3), // Pre-allocate event slice
		}),
	}
	jr.ctx, jr.cancel = context.WithCancel(ctx)
	jr.logger = loggerFactory.New(jr.logsBuf)
	return jr
}

func (j *jobRunInner) addEvent(evt JobEvent) {
	j.events = append(j.events, evt)
}

func (j *jobRunStruct) export() JobRun {
	var innerCopy jobRunInner
	j.inner.RWith(func(inner jobRunInner) {
		innerCopy = inner
		innerCopy.events = slices.Clone(inner.events) // Snapshot must not share memory with the run
	})
	return JobRun{
		RunID:       j.runID,
		Entry:       j.entry,
		CreatedAt:   j.createdAt,
		Logs:        j.logsBuf.String(),
		logger:      j.logger,
		Events:      innerCopy.events,
		StartedAt:   innerCopy.startedAt,
		CompletedAt: innerCopy.completedAt,
		Error:       innerCopy.error,
		Panic:       innerCopy.panic,
	}
}
