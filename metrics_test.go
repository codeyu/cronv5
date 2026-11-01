package cron

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type recordedMetric struct {
	kind     string
	entryID  EntryID
	run      JobRun
	dur      time.Duration
	reason   SkipReason
	schedule time.Time
}

// testMetrics records the calls, only JobStarted / JobCompleted are implemented to check BaseMetrics
type testMetrics struct {
	BaseMetrics
	mu    sync.Mutex
	calls []recordedMetric
}

func (m *testMetrics) record(r recordedMetric) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, r)
}

func (m *testMetrics) JobStarted(run JobRun, delay time.Duration) {
	m.record(recordedMetric{kind: "started", entryID: run.Entry.ID, run: run, dur: delay})
}

func (m *testMetrics) JobCompleted(run JobRun, duration time.Duration) {
	m.record(recordedMetric{kind: "completed", entryID: run.Entry.ID, run: run, dur: duration})
}

func (m *testMetrics) get() []recordedMetric {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]recordedMetric(nil), m.calls...)
}

// skipMetrics also records the skipped activations
type skipMetrics struct{ testMetrics }

func (m *skipMetrics) JobSkipped(entry Entry, scheduledAt time.Time, reason SkipReason) {
	m.record(recordedMetric{kind: "skipped", entryID: entry.ID, schedule: scheduledAt, reason: reason})
}

func TestMetrics(t *testing.T) {
	metrics := &testMetrics{}
	cron, clock := newRegressionCron(func(b *Builder) { b.WithMetrics(metrics) })
	cron.Start()
	defer cron.Stop()
	start := clock.Now()
	id, _ := cron.AddJob("*/10 * * * * *", func() error {
		clock.Sleep(0)
		return errors.New("failed")
	}, WithID("job"))

	advanceAndCycle(cron, 10*time.Second)
	calls := metrics.get()
	if assert.Len(t, calls, 2) {
		assert.Equal(t, "started", calls[0].kind)
		assert.Equal(t, EntryID("job"), calls[0].entryID)
		assert.Equal(t, start.Add(10*time.Second), calls[0].run.ScheduledAt)
		assert.False(t, calls[0].run.Triggered)
		assert.Equal(t, time.Duration(0), calls[0].dur) // not late
		assert.Equal(t, "completed", calls[1].kind)
		assert.EqualError(t, calls[1].run.Error, "failed")
		assert.NotNil(t, calls[1].run.CompletedAt)
	}

	// Triggered run: scheduled at the time of Trigger
	advanceAndCycle(cron, 3*time.Second)
	_, err := cron.Trigger(id)
	assert.NoError(t, err)
	cron.waitAllJobsCompleted()
	calls = metrics.get()
	if assert.Len(t, calls, 4) {
		assert.True(t, calls[2].run.Triggered)
		assert.Equal(t, start.Add(13*time.Second), calls[2].run.ScheduledAt)
	}
}

// The delay is how late the run started compared to its scheduled time
func TestMetricsDelay(t *testing.T) {
	metrics := &testMetrics{}
	cron, clock := newRegressionCron(func(b *Builder) { b.WithMetrics(metrics) })
	scheduled := clock.Now().Add(time.Minute)
	_, _ = cron.AddJob("0 0 0 1 1 *", func() {}, WithNext(scheduled))
	clock.Advance(5 * time.Minute) // the cron starts late
	cron.Start()
	defer cron.Stop()
	cycle(cron)
	cron.waitAllJobsCompleted()
	calls := metrics.get()
	if assert.NotEmpty(t, calls) {
		assert.Equal(t, scheduled, calls[0].run.ScheduledAt)
		assert.Equal(t, 4*time.Minute, calls[0].dur)
	}
}

type panickingMetrics struct{ BaseMetrics }

func (panickingMetrics) JobStarted(JobRun, time.Duration) { panic("metrics bug") }

func TestMetricsPanicIsRecovered(t *testing.T) {
	cron, _ := newRegressionCron(func(b *Builder) { b.WithMetrics(panickingMetrics{}) })
	cron.Start()
	defer cron.Stop()
	ran := make(chan struct{}, 1)
	id, _ := cron.AddJob("0 0 0 1 1 *", func() { ran <- struct{}{} })
	_, err := cron.Trigger(id)
	assert.NoError(t, err)
	cron.waitAllJobsCompleted()
	select {
	case <-ran: // the job still runs
	default:
		t.Fatal("job did not run")
	}
}

func TestSkipReasonString(t *testing.T) {
	assert.Equal(t, "misfire", SkipMisfire.String())
	assert.Equal(t, "still running", SkipStillRunning.String())
	assert.Equal(t, "unknown", SkipReason(0).String())
}

var _ Metrics = BaseMetrics{}
