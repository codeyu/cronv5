package cron

import (
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"log/slog"
	"runtime/debug"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/codeyu/cronv5/internal/mtx"
	"github.com/codeyu/cronv5/internal/pubsub"
	isync "github.com/codeyu/cronv5/internal/sync"
	"github.com/codeyu/cronv5/internal/utils"
	"github.com/jonboulle/clockwork"
)

// Cron keeps track of any number of entries, invoking the associated func as
// specified by the schedule. It may be started, stopped, and the entries may
// be inspected while running.
type Cron struct {
	clock                clockwork.Clock                              // Clock interface (real or mock) used for timing
	runningJobsCount     atomic.Int32                                 // Count of currently running jobs
	runningJobsMap       isync.Map[EntryID, *mtx.RWMtx[jobRunsInner]] // Keep track of currently running jobs
	cond                 sync.Cond                                    // Signals when all jobs have completed
	entries              mtx.RWMtx[entries]                           // Thread-safe, sorted list of job entries
	parentCtx            context.Context                              // Parent context provided by the user
	lifecycle            atomic.Pointer[lifecycle]                    // Context of the current run (renewed when restarting after a Stop)
	lifecycleMu          sync.Mutex                                   // Serializes start/stop transitions
	update               chan context.CancelFunc                      // Triggers update in the scheduler loop, and wait for it to be processed
	notify               chan struct{}                                // Triggers update in the scheduler loop, without waiting
	running              atomic.Bool                                  // Indicates if the scheduler is currently running
	location             atomic.Pointer[time.Location]                // Thread-safe time zone location
	logger               Logger                                       // Logger for scheduler events, errors, and diagnostics
	parser               ScheduleParser                               // Parses cron expressions into schedule objects
	idFactory            IDFactory                                    // Generates a new unique ID for Entry/JobRun/Hook
	jobRunLoggerFactory  JobRunLoggerFactory                          // Factory for creating loggers for individual job runs
	ps                   *pubsub.PubSub[EntryID, JobEvent]            // PubSub for publishing and subscribing to job events
	jobRunCreatedCh      chan JobRun                                  // Channel for receiving notifications when new job runs are created
	jobRunCompletedCh    chan JobRun                                  // Channel for receiving notifications when job runs complete
	keepCompletedRunsDur mtx.Mtx[time.Duration]                       // Duration to keep completed job runs before cleanup (thread-safe)
	maxCompletedRuns     int                                          // Maximum number of completed job runs kept per entry (0 = unlimited)
	maxLogSize           int                                          // Maximum size in bytes of the logs kept per job run (0 = unlimited)
	keepJobsOnStop       bool                                         // Stop does not cancel the context of the running jobs
	metrics              Metrics                                      // Receives the measurements of the job runs (nil = none)
	misfirePolicy        MisfirePolicy                                // Policy of the entries using MisfireDefault
	misfireThreshold     time.Duration                                // An activation is missed when it runs later than this
	limits               concurrencyLimits                            // Concurrency limits of the job runs
	cleanupNowCh         chan struct{}                                // Channel to trigger immediate cleanup of completed job runs
	lastCleanupTS        mtx.Mtx[time.Time]                           // Timestamp of last completed job runs cleanup (thread-safe)
	hooks                mtx.RWMtx[hooksContainer]                    // Thread-safe container for managing job hooks (both global and entry-specific)
}

// lifecycle holds the contexts of one run of the cron (between a Start and a Stop)
type lifecycle struct {
	ctx        context.Context // Used by the scheduler loop and the cleanup thread
	cancel     context.CancelFunc
	jobsCtx    context.Context // Parent of the job runs contexts, cancelled separately so Stop can keep jobs running
	cancelJobs context.CancelFunc
}

func newLifecycle(parentCtx context.Context) *lifecycle {
	ctx, cancel := context.WithCancel(parentCtx)
	jobsCtx, cancelJobs := context.WithCancel(parentCtx)
	return &lifecycle{ctx: ctx, cancel: cancel, jobsCtx: jobsCtx, cancelJobs: cancelJobs}
}

type hooksContainer struct {
	hooksMap       map[HookID]hookMeta
	globalHooksMap map[JobEventType][]*hookStruct
	entryHooksMap  map[EntryID]map[JobEventType][]*hookStruct
}

type hookMeta struct {
	hook    *hookStruct
	evt     JobEventType
	entryID *EntryID
}

func (c *hooksContainer) hooksFor(evt JobEventType, entryID EntryID) []*hookStruct {
	// Do not append to the global slice directly, it would write into its backing array
	return slices.Concat(c.globalHooksMap[evt], c.entryHooksMap[entryID][evt])
}

func (c *hooksContainer) iterHooks() iter.Seq[hookMeta] {
	return func(yield func(hookMeta) bool) {
		for evt, hooks := range c.globalHooksMap {
			for _, hook := range hooks {
				if !yield(hookMeta{hook, evt, nil}) {
					return
				}
			}
		}
		for entryID, evtMap := range c.entryHooksMap {
			for evt, hooks := range evtMap {
				for _, hook := range hooks {
					if !yield(hookMeta{hook, evt, &entryID}) {
						return
					}
				}
			}
		}
	}
}

func (c *hooksContainer) addHook(evt JobEventType, hook *hookStruct) {
	c.globalHooksMap[evt] = append(c.globalHooksMap[evt], hook)
	c.addMetaLookup(hook, evt, nil)
}

func (c *hooksContainer) addEntryHook(entryID EntryID, evt JobEventType, hook *hookStruct) {
	if c.entryHooksMap[entryID] == nil {
		c.entryHooksMap[entryID] = make(map[JobEventType][]*hookStruct)
	}
	c.entryHooksMap[entryID][evt] = append(c.entryHooksMap[entryID][evt], hook)
	c.addMetaLookup(hook, evt, &entryID)
}

func (c *hooksContainer) addMetaLookup(hook *hookStruct, evt JobEventType, entryID *EntryID) {
	c.hooksMap[hook.id] = hookMeta{hook, evt, entryID}
}

func (c *hooksContainer) removeEntryHooks(entryID EntryID) {
	for _, hooks := range c.entryHooksMap[entryID] {
		for _, hook := range hooks {
			delete(c.hooksMap, hook.id)
		}
	}
	delete(c.entryHooksMap, entryID)
}

type jobRunsInner struct {
	mapping     map[RunID]*jobRunStruct
	running     []*jobRunStruct
	completed   []*jobRunStruct
	completedTS []time.Time
}

type entries struct {
	heap       *entryHeap
	entriesMap map[EntryID]*Entry
}

// ErrHookNotFound ...
var ErrHookNotFound = errors.New("hook not found")

// ErrEntryNotFound ...
var ErrEntryNotFound = errors.New("entry not found")

// ErrJobRunNotFound ...
var ErrJobRunNotFound = errors.New("job run not found")

// ErrUnsupportedJobType ...
var ErrUnsupportedJobType = errors.New("unsupported job type")

// ErrJobAlreadyRunning ...
var ErrJobAlreadyRunning = errors.New("job already running")

// ErrIDAlreadyUsed ...
var ErrIDAlreadyUsed = errors.New("id already used")

// ErrCronNotRunning is returned by Trigger when the cron is not running
var ErrCronNotRunning = errors.New("cron is not running")

// ErrUnsatisfiableSchedule is returned when a spec never activates (eg: "0 0 30 2 *", February 30th)
var ErrUnsatisfiableSchedule = errors.New("schedule never activates")

// The Schedule describes a job's duty cycle.
type Schedule interface {
	// Next return the next activation time, later than the given time.
	// Next is invoked initially, and then each time the job is run.
	Next(time.Time) time.Time
}

// ScheduleParser is an interface for schedule spec parsers that return a Schedule
type ScheduleParser interface {
	Parse(spec string) (Schedule, error)
}

// ID is the type to use for all internal IDs (EntryID, RunID, HookID)
type ID string

// FuncIDFactory is a function type that generates a new unique ID.
// It implements the IDFactory interface by providing a Next() method.
type FuncIDFactory func() ID

// Next implements the IDFactory interface for FuncIDFactory.
// It calls the underlying function to generate and return a new ID.
func (f FuncIDFactory) Next() ID { return f() }

// IDFactory is an interface for types that can generate unique IDs.
// Implementations should provide thread-safe ID generation.
type IDFactory interface {
	// Next generates and returns a new unique ID.
	// Implementations must ensure IDs are unique across calls.
	Next() ID
}

// UuidIDFactory generate and format UUID V4
func UuidIDFactory() IDFactory {
	return FuncIDFactory(func() ID {
		return ID(utils.UuidV4Str())
	})
}

// JobRunLoggerFactory is an interface for creating loggers for individual job runs.
// Implementations should create a new logger that writes to the provided io.Writer.
type JobRunLoggerFactory interface {
	// New creates a new slog.Logger instance that writes to the given io.Writer.
	New(w io.Writer) Logger
}

// FuncJobRunLoggerFactory is a function type that implements JobRunLoggerFactory.
// It allows any function with the signature func(w io.Writer) *slog.Logger
// to be used as a JobRunLoggerFactory.
type FuncJobRunLoggerFactory func(w io.Writer) Logger

// New implements the JobRunLoggerFactory interface for FuncJobRunLoggerFactory.
// It simply calls the underlying function with the provided writer.
func (f FuncJobRunLoggerFactory) New(w io.Writer) Logger { return f(w) }

// DefaultJobRunLoggerFactory returns a default implementation of JobRunLoggerFactory.
// The default implementation creates a text-based logger with debug level logging
// that writes to the provided io.Writer.
func DefaultJobRunLoggerFactory() JobRunLoggerFactory {
	return FuncJobRunLoggerFactory(func(w io.Writer) Logger {
		return slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{Level: slog.LevelDebug}))
	})
}

// -----------------------------------------------------------------------------

// New creates a new Builder instance for constructing a Cron scheduler.
func New() *Builder {
	return &Builder{}
}

// Builder provides a fluent interface for constructing a Cron instance with optional configuration.
// Use New() to create a builder, chain configuration methods, and call Build() to create the Cron instance.
type Builder struct {
	clock                clockwork.Clock
	location             *time.Location
	parser               ScheduleParser
	idFactory            IDFactory
	jobRunLoggerFactory  JobRunLoggerFactory
	keepCompletedRunsDur *time.Duration
	maxCompletedRuns     int
	maxLogSize           int
	keepJobsOnStop       bool
	metrics              Metrics
	misfirePolicy        MisfirePolicy
	misfireThreshold     *time.Duration
	maxConcurrentJobs    int
	tagLimits            map[string]int
	logger               Logger
	ctx                  context.Context
}

// WithClock sets the clock implementation for the Cron instance.
func (b *Builder) WithClock(clock clockwork.Clock) *Builder {
	b.clock = clock
	return b
}

// WithLocation sets the timezone location for the Cron instance.
func (b *Builder) WithLocation(loc *time.Location) *Builder {
	b.location = loc
	return b
}

// WithParser sets the schedule parser for the Cron instance.
func (b *Builder) WithParser(parser ScheduleParser) *Builder {
	b.parser = parser
	return b
}

// WithSeconds configures the parser to include seconds in the schedule specification.
func (b *Builder) WithSeconds() *Builder {
	b.parser = SecondParser
	return b
}

// WithIDFactory sets the ID generator factory for the Cron instance.
func (b *Builder) WithIDFactory(idFactory IDFactory) *Builder {
	b.idFactory = idFactory
	return b
}

// WithJobRunLoggerFactory sets the logger factory for individual job runs.
func (b *Builder) WithJobRunLoggerFactory(jobRunLoggerFactory JobRunLoggerFactory) *Builder {
	b.jobRunLoggerFactory = jobRunLoggerFactory
	return b
}

// WithKeepCompletedRunsDur sets the duration to keep completed job runs before cleanup.
func (b *Builder) WithKeepCompletedRunsDur(keepCompletedRunsDur time.Duration) *Builder {
	b.keepCompletedRunsDur = &keepCompletedRunsDur
	return b
}

// WithMaxCompletedRunsPerEntry sets the maximum number of completed job runs kept per entry,
// the oldest ones are freed first. Completed job runs are kept if this option, or WithKeepCompletedRunsDur, is set.
func (b *Builder) WithMaxCompletedRunsPerEntry(n int) *Builder {
	b.maxCompletedRuns = max(n, 0)
	return b
}

// WithMaxLogSize sets the maximum size in bytes of the logs kept for each job run.
// When the limit is exceeded, the oldest logs are discarded.
func (b *Builder) WithMaxLogSize(n int) *Builder {
	b.maxLogSize = max(n, 0)
	return b
}

// KeepJobsRunningOnStop makes Stop wait for the running jobs without cancelling their context.
// By default, Stop cancels the context of the running jobs.
func (b *Builder) KeepJobsRunningOnStop() *Builder {
	b.keepJobsOnStop = true
	return b
}

// WithLogger sets the logger for the Cron instance.
func (b *Builder) WithLogger(logger Logger) *Builder {
	b.logger = logger
	return b
}

// Quiet configures the builder to discard all log output
func (b *Builder) Quiet() *Builder {
	b.logger = DiscardLogger
	return b
}

// WithContext sets the base context for the Cron instance.
func (b *Builder) WithContext(ctx context.Context) *Builder {
	b.ctx = ctx
	return b
}

// Build constructs and returns a configured Cron instance using the builder's settings.
// Default values are used for any unconfigured properties.
func (b *Builder) Build() *Cron {
	clock := utils.Or(b.clock, clockwork.NewRealClock())
	location := utils.Or(b.location, clock.Now().Location())
	parentCtx := utils.Or(b.ctx, context.Background())
	logger := utils.Or(b.logger, DefaultLogger)
	parser := utils.Or(b.parser, ScheduleParser(standardParser))
	idFactory := utils.Or(b.idFactory, UuidIDFactory())
	jobRunLoggerFactory := utils.Or(b.jobRunLoggerFactory, DefaultJobRunLoggerFactory())
	keepCompletedRunsDur := utils.Default(b.keepCompletedRunsDur, 0)
	c := &Cron{
		cond:                 sync.Cond{L: &sync.Mutex{}},
		clock:                clock,
		parentCtx:            parentCtx,
		runningJobsMap:       isync.Map[EntryID, *mtx.RWMtx[jobRunsInner]]{},
		update:               make(chan context.CancelFunc),
		notify:               make(chan struct{}, 1),
		logger:               logger,
		parser:               parser,
		idFactory:            idFactory,
		jobRunLoggerFactory:  jobRunLoggerFactory,
		ps:                   pubsub.NewPubSub[EntryID, JobEvent](),
		jobRunCreatedCh:      make(chan JobRun),
		jobRunCompletedCh:    make(chan JobRun),
		keepCompletedRunsDur: mtx.NewMtx(keepCompletedRunsDur),
		maxCompletedRuns:     b.maxCompletedRuns,
		maxLogSize:           b.maxLogSize,
		keepJobsOnStop:       b.keepJobsOnStop,
		metrics:              b.metrics,
		misfirePolicy:        b.misfirePolicy,
		misfireThreshold:     utils.Default(b.misfireThreshold, defaultMisfireThreshold),
		limits:               newConcurrencyLimits(b.maxConcurrentJobs, b.tagLimits),
		cleanupNowCh:         make(chan struct{}, 1),
		hooks: mtx.NewRWMtx(hooksContainer{
			hooksMap:       make(map[HookID]hookMeta),
			globalHooksMap: make(map[JobEventType][]*hookStruct),
			entryHooksMap:  make(map[EntryID]map[JobEventType][]*hookStruct),
		}),
		entries: mtx.NewRWMtx(entries{
			heap:       newEntryHeap(),
			entriesMap: make(map[EntryID]*Entry),
		}),
	}
	c.location.Store(location)
	c.lifecycle.Store(newLifecycle(parentCtx))
	return c
}

// Run the cron scheduler, or no-op if already running.
func (c *Cron) Run() (started bool) { return c.start() }

// Start the cron scheduler in its own go-routine, or no-op if already started.
func (c *Cron) Start() (started bool) { return c.startAsync() }

// Stop stops the cron scheduler if it is running.
// A channel is returned so the caller can wait for running jobs to complete.
// The cron can be started again after being stopped.
func (c *Cron) Stop() <-chan struct{} { return c.stop() }

// Wait stops the cron and waits for all running jobs to complete before returning.
func (c *Cron) Wait() { c.wait() }

// Shutdown gracefully stops the cron: no new job is started, and it waits for the running jobs to complete
// without cancelling them. If ctx is done before, the context of the running jobs is cancelled
// and ctx.Err() is returned (without waiting for jobs that ignore their context).
func (c *Cron) Shutdown(ctx context.Context) error { return c.shutdown(ctx) }

// OnEvt registers a global hook function for a specific job event type.
// The hook will be called whenever the specified event occurs for any job.
// Returns the HookID for later removal.
func (c *Cron) OnEvt(evt JobEventType, clb HookFn, opts ...HookOption) HookID {
	return c.onEvt(evt, clb, opts...)
}

// OnEntryEvt registers a hook function for a specific job event type on a specific entry.
// The hook will be called only when the specified event occurs for the given entry.
// Returns the HookID for later removal.
func (c *Cron) OnEntryEvt(entryID EntryID, evt JobEventType, clb HookFn, opts ...HookOption) HookID {
	return c.onEntryEvt(entryID, evt, clb, opts...)
}

// RemoveHook removes a previously registered hook by its HookID.
// Works for both global and entry-specific hooks.
func (c *Cron) RemoveHook(id HookID) { c.removeHook(id) }

// EnableHook enables a previously disabled hook by its ID
func (c *Cron) EnableHook(id HookID) { c.enableHook(id) }

// DisableHook disables a previously enabled hook by its ID
func (c *Cron) DisableHook(id HookID) { c.disableHook(id) }

// GetHooks returns all registered hooks (both global and entry-specific)
func (c *Cron) GetHooks() []Hook { return c.getHooks() }

// GetHook returns a specific hook by ID or ErrHookNotFound if not found
func (c *Cron) GetHook(id HookID) (Hook, error) { return c.getHook(id) }

// SetHookLabel sets an hook's label
func (c *Cron) SetHookLabel(id HookID, label string) { c.setHookLabel(id, label) }

// OnJobStart registers a global hook function for job start events.
// The hook will be called whenever any job starts.
// Returns the HookID for later removal.
func (c *Cron) OnJobStart(clb HookFn, opts ...HookOption) HookID { return c.onJobStart(clb, opts...) }

// OnEntryJobStart registers a hook function for job start events on a specific entry.
// The hook will be called only when the given entry's job starts.
// Returns the HookID for later removal.
func (c *Cron) OnEntryJobStart(entryID EntryID, clb HookFn, opts ...HookOption) HookID {
	return c.onEntryJobStart(entryID, clb, opts...)
}

// OnJobCompleted registers a global hook function for job completion events.
// The hook will be called whenever any job completes.
// Returns the HookID for later removal.
func (c *Cron) OnJobCompleted(clb HookFn, opts ...HookOption) HookID {
	return c.onJobCompleted(clb, opts...)
}

// OnEntryJobCompleted registers a hook function for job completion events on a specific entry.
// The hook will be called only when the given entry's job completes.
// Returns the HookID for later removal.
func (c *Cron) OnEntryJobCompleted(entryID EntryID, clb HookFn, opts ...HookOption) HookID {
	return c.onEntryJobCompleted(entryID, clb, opts...)
}

// AddJob adds a Job to the Cron to be run on the given schedule.
func (c *Cron) AddJob(spec string, job IntoJob, opts ...EntryOption) (EntryID, error) {
	return c.addJob(spec, job, opts...)
}

// AddJobStrict same as AddJob, but only accept a Job interface instead of IntoJob (any)
func (c *Cron) AddJobStrict(spec string, job Job, opts ...EntryOption) (EntryID, error) {
	return c.addJobStrict(spec, job, opts...)
}

// AddFunc adds a Job to the Cron to be run on the given schedule.
func (c *Cron) AddFunc(spec string, job FuncJob, opts ...EntryOption) (EntryID, error) {
	return c.addFunc(spec, job, opts...)
}

// Schedule adds a Job to the Cron to be run on the given schedule.
func (c *Cron) Schedule(schedule Schedule, job Job, opts ...EntryOption) (EntryID, error) {
	return c.schedule(nil, schedule, job, opts...)
}

// AddEntry adds a pre-configured Entry to the Cron
func (c *Cron) AddEntry(entry Entry, opts ...EntryOption) (EntryID, error) {
	return c.addEntry(entry, opts...)
}

// UpdateSchedule updates an entry's schedule with a new Schedule
func (c *Cron) UpdateSchedule(id EntryID, schedule Schedule) error {
	return c.updateSchedule(id, nil, schedule)
}

// UpdateScheduleWithSpec updates an entry's schedule by parsing a spec string
func (c *Cron) UpdateScheduleWithSpec(id EntryID, spec string) error {
	return c.updateScheduleWithSpec(id, spec)
}

// UpdateLabel updates an entry's label
func (c *Cron) UpdateLabel(id EntryID, label string) { c.updateLabel(id, label) }

// Sub subscribes to job events for a specific entry ID
func (c *Cron) Sub(id EntryID) *pubsub.Sub[EntryID, JobEvent] {
	return c.ps.Subscribe([]EntryID{id})
}

// JobRunCreatedCh returns channel for job run creation notifications
func (c *Cron) JobRunCreatedCh() <-chan JobRun { return c.jobRunCreatedCh }

// JobRunCompletedCh returns channel for job run completion notifications
func (c *Cron) JobRunCompletedCh() <-chan JobRun { return c.jobRunCompletedCh }

// Enable activates a previously disabled cron entry by its ID
func (c *Cron) Enable(id EntryID) { c.setEntryActive(id, true) }

// Disable deactivates a previously enabled cron entry by its ID
func (c *Cron) Disable(id EntryID) { c.setEntryActive(id, false) }

// Entries returns a snapshot of the cron entries.
func (c *Cron) Entries() []Entry { return c.getEntries() }

// Entry returns a snapshot of the given entry, or nil if it couldn't be found.
func (c *Cron) Entry(id EntryID) (Entry, error) { return c.getEntry(id) }

// Remove an entry from being run in the future.
// Running jobs of the entry are cancelled, and the hooks specific to the entry are removed.
func (c *Cron) Remove(id EntryID) { c.remove(id) }

// EntriesByTag returns a snapshot of the entries having the given tag, sorted like Entries
func (c *Cron) EntriesByTag(tag string) []Entry { return c.entriesByTag(tag) }

// RemoveByTag removes the entries having the given tag (as Remove does), and returns how many were removed
func (c *Cron) RemoveByTag(tag string) int { return c.forEachTagged(tag, c.remove) }

// EnableByTag enables the entries having the given tag, and returns how many entries have the tag
func (c *Cron) EnableByTag(tag string) int { return c.forEachTagged(tag, c.Enable) }

// DisableByTag disables the entries having the given tag, and returns how many entries have the tag
func (c *Cron) DisableByTag(tag string) int { return c.forEachTagged(tag, c.Disable) }

// SetTags replaces the tags of an entry. The concurrency limits of the tags (WithTagConcurrencyLimit)
// apply to the runs started after the change.
func (c *Cron) SetTags(id EntryID, tags ...string) error { return c.setTags(id, tags) }

// RunNow runs the job of the entry now, as Trigger does, without returning the RunID.
//
// If the cron is not running, the job runs as soon as the cron is started
// (unless the entry is disabled).
func (c *Cron) RunNow(id EntryID) error { return c.runNow(id) }

// Trigger runs the job of the entry now, in addition to its schedule: the next activation
// time of the entry (Entry.Next) is not changed. The run is like a scheduled one: the job
// wrappers, hooks, events and panic recovery apply, and the run can be followed with its
// RunID (GetJobRun, CancelJobRun...). The run is started when Trigger returns.
//
// Disabled entries are run too. It returns ErrCronNotRunning if the cron is not running,
// and ErrEntryNotFound if the entry does not exist.
func (c *Cron) Trigger(id EntryID) (RunID, error) { return c.trigger(id) }

// IsRunning either or not a specific job is currently running
func (c *Cron) IsRunning(id EntryID) bool { return c.entryIsRunning(id) }

// RunningJobs returns a list of all currently running jobs across all entries
func (c *Cron) RunningJobs() []JobRun { return c.runningJobs() }

// CompletedJobs returns a list of all completed job runs across all entries
func (c *Cron) CompletedJobs() []JobRun { return c.completedJobs() }

// RecentCompletedJobs returns the "limit" most recent completed job runs across all entries, newest first.
// Only the returned runs are exported, which is cheaper than CompletedJobs when many runs are kept.
// A limit <= 0 returns all the completed job runs.
func (c *Cron) RecentCompletedJobs(limit int) []JobRun { return c.recentCompletedJobs(limit) }

// RunningJobsFor returns all currently running jobs for a specific entry
// // Returns ErrEntryNotFound if the entry doesn't exist.
func (c *Cron) RunningJobsFor(entryID EntryID) ([]JobRun, error) {
	return c.runningJobsFor(entryID)
}

// CompletedJobRunsFor returns all completed job runs for a specific entry.
// Returns ErrEntryNotFound if the entry doesn't exist.
func (c *Cron) CompletedJobRunsFor(entryID EntryID) ([]JobRun, error) {
	return c.completedJobRunsFor(entryID)
}

// GetJobRun retrieves a specific job run by entry ID and run ID.
// Returns ErrEntryNotFound if the entry doesn't exist or ErrJobRunNotFound if the run doesn't exist.
func (c *Cron) GetJobRun(entryID EntryID, runID RunID) (JobRun, error) {
	return c.getJobRun(entryID, runID)
}

// CancelJobRun attempts to cancel a running job by entry ID and run ID.
// Returns ErrEntryNotFound if the entry doesn't exist or ErrJobRunNotFound if the run doesn't exist.
func (c *Cron) CancelJobRun(entryID EntryID, runID RunID) error { return c.cancelRun(entryID, runID) }

// Location gets the time zone location
func (c *Cron) Location() *time.Location { return c.getLocation() }

// SetLocation sets a new location to use.
// Re-set the "Next" values for all entries.
// Re-sort entries and run due entries.
func (c *Cron) SetLocation(newLoc *time.Location) { c.setLocation(newLoc) }

// GetNextTime returns the next time a job is scheduled to be executed
// If no job is scheduled to be executed, the Zero time is returned
func (c *Cron) GetNextTime() time.Time { return c.getNextTime() }

// GetCleanupTS returns the timestamp of the last completed job runs cleanup
func (c *Cron) GetCleanupTS() time.Time { return c.lastCleanupTS.Get() }

// CleanupNow triggers immediate cleanup of completed job runs
func (c *Cron) CleanupNow() { c.cleanupNow() }

// SetCleanupInterval sets the duration for keeping completed job runs before cleanup
func (c *Cron) SetCleanupInterval(dur time.Duration) { c.setCleanupInterval(dur) }

//-----------------------------------------------------------------------------

// cleanupLoop periodically frees the completed job runs older than keepCompletedRunsDur.
// There is a single cleanup loop per run of the cron, it stops when the cron is stopped.
func (c *Cron) cleanupLoop(ctx context.Context) {
	defer c.logger.Info("cleanup thread stopped")
	for {
		var tickCh <-chan time.Time
		var timer clockwork.Timer
		if keepCompletedRunsDur := c.keepCompletedRunsDur.Get(); keepCompletedRunsDur > 0 {
			timer = c.clock.NewTimer(keepCompletedRunsDur)
			tickCh = timer.Chan()
		}
		cleanupNowReceived := false
		select {
		case <-tickCh:
		case <-c.cleanupNowCh:
			cleanupNowReceived = true
		case <-ctx.Done():
		}
		if timer != nil {
			timer.Stop()
		}
		if ctx.Err() != nil { // Cron stopped, possibly restarted with a new cleanup loop
			if cleanupNowReceived {
				c.cleanupNow() // Hand the request over to the next cleanup loop
			}
			return
		}
		// When only the max number of completed runs is set, runs are not cleaned up by age
		if keepCompletedRunsDur := c.keepCompletedRunsDur.Get(); keepCompletedRunsDur > 0 || c.maxCompletedRuns == 0 {
			c.cleanupOldRuns(ctx, keepCompletedRunsDur)
		}
	}
}

func (c *Cron) cleanupOldRuns(ctx context.Context, keepCompletedRunsDur time.Duration) {
	var totalCleaned int
	start := c.clock.Now()
	thresholdTime := start.Add(-keepCompletedRunsDur)
	for v := range c.runningJobsMap.IterValues() {
		if ctx.Err() != nil {
			return
		}
		v.With(func(inner *jobRunsInner) {
			idx := sort.Search(len(inner.completedTS), func(i int) bool {
				return inner.completedTS[i].After(thresholdTime)
			})
			if idx > 0 {
				freeOldestCompletedRuns(inner, idx)
				totalCleaned += idx
			}
		})
	}
	if totalCleaned > 0 {
		c.logger.Debug("cleaned old runs", "count", totalCleaned, "duration", c.clock.Since(start))
	}
	c.lastCleanupTS.Set(c.clock.Now())
}

func (c *Cron) cleanupNow() {
	utils.NonBlockingSend(c.cleanupNowCh, struct{}{})
}

func (c *Cron) setCleanupInterval(dur time.Duration) {
	c.keepCompletedRunsDur.Set(dur)
	c.cleanupNow() // Wake up the cleanup loop so it picks up the new interval
}

func maybeAsync(fn func(), async bool) func() {
	return utils.Ternary(async, func() { go fn() }, func() { fn() })
}

func (c *Cron) start() (started bool) {
	return c.startWith(maybeAsync(c.run, false)) // sync
}

func (c *Cron) startAsync() (started bool) {
	return c.startWith(maybeAsync(c.run, true)) // async
}

func (c *Cron) startWith(runFunc func()) (started bool) {
	if started = c.startRunning(); started {
		runFunc()
	}
	return
}

func (c *Cron) stop() <-chan struct{} {
	lc := c.stopRunning()
	if !c.keepJobsOnStop {
		lc.cancelJobs()
	}
	ch := make(chan struct{})
	go func() {
		c.waitAllJobsCompleted()
		lc.cancelJobs() // Release resources
		close(ch)
	}()
	return ch
}

func (c *Cron) shutdown(ctx context.Context) error {
	lc := c.stopRunning()
	done := make(chan struct{})
	go func() {
		c.waitAllJobsCompleted()
		close(done)
	}()
	defer lc.cancelJobs()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Cron) wait() {
	<-c.stop()
}

// ctx returns the context of the current run of the cron
func (c *Cron) ctx() context.Context {
	return c.lifecycle.Load().ctx
}

func (c *Cron) startRunning() bool {
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()
	if c.running.Load() {
		return false
	}
	if c.ctx().Err() != nil { // Restarting after a Stop
		c.lifecycle.Store(newLifecycle(c.parentCtx))
	}
	c.refreshStaleEntries()
	c.running.Store(true)
	go c.cleanupLoop(c.ctx())
	return true
}

// stopRunning stops the scheduler, and returns the lifecycle of the run that was stopped
// (or of the last run if the cron was not running)
func (c *Cron) stopRunning() *lifecycle {
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()
	lc := c.lifecycle.Load()
	if c.running.Load() {
		c.running.Store(false)
		lc.cancel()
	}
	return lc
}
func (c *Cron) isRunning() bool {
	return c.running.Load()
}

func (c *Cron) makeHook(clb HookFn, opts ...HookOption) *hookStruct {
	hook := hookFunc(c.idFactory, clb)
	utils.ApplyOptions(hook, opts...)
	return hook
}

func (c *Cron) onEvt(evt JobEventType, clb HookFn, opts ...HookOption) HookID {
	hook := c.makeHook(clb, opts...)
	c.hooks.With(func(v *hooksContainer) { v.addHook(evt, hook) })
	return hook.id
}

func (c *Cron) onEntryEvt(entryID EntryID, evt JobEventType, clb HookFn, opts ...HookOption) HookID {
	hook := c.makeHook(clb, opts...)
	c.hooks.With(func(v *hooksContainer) { v.addEntryHook(entryID, evt, hook) })
	return hook.id
}

func (c *Cron) removeHook(id HookID) {
	c.hooks.With(func(v *hooksContainer) {
		for evt, hooks := range v.globalHooksMap {
			if hook, idx := utils.FindIdx(hooks, func(h *hookStruct) bool { return h.id == id }); idx != -1 {
				v.globalHooksMap[evt] = slices.Delete(v.globalHooksMap[evt], idx, idx+1)
				delete(v.hooksMap, (*hook).id)
				if len(v.globalHooksMap[evt]) == 0 {
					delete(v.globalHooksMap, evt)
				}
				return
			}
		}
		for entryID, eventMap := range v.entryHooksMap {
			for evt, hooks := range eventMap {
				if hook, idx := utils.FindIdx(hooks, func(h *hookStruct) bool { return h.id == id }); idx != -1 {
					eventMap[evt] = slices.Delete(eventMap[evt], idx, idx+1)
					delete(v.hooksMap, (*hook).id)
					if len(eventMap[evt]) == 0 {
						delete(eventMap, evt)
					}
					if len(eventMap) == 0 {
						delete(v.entryHooksMap, entryID)
					}
					return
				}
			}
		}
	})
}

func (c *Cron) getHooks() (out []Hook) {
	c.hooks.RWith(func(v hooksContainer) {
		for hook := range v.iterHooks() {
			out = append(out, hook.hook.export(hook.evt, hook.entryID))
		}
	})
	return
}

func (c *Cron) hookClb(id HookID, clb func(meta hookMeta)) {
	c.hooks.With(func(v *hooksContainer) {
		if hook, ok := v.hooksMap[id]; ok {
			clb(hook)
		}
	})
}

func lookupClb[K comparable, V any, M map[K]V](m M, k K, clb func(V)) bool {
	hook, ok := m[k]
	if ok {
		clb(hook)
	}
	return ok
}

func lookupErrClb[K comparable, V any, M map[K]V](m M, k K, clb func(V), e error) error {
	return utils.TernaryOrZero(!lookupClb(m, k, clb), e)
}

func (c *Cron) hookRClb(id HookID, clb func(hookMeta)) error {
	return c.hooks.RWithE(func(v hooksContainer) error {
		return lookupErrClb(v.hooksMap, id, clb, ErrHookNotFound)
	})
}

func (c *Cron) entryRClb(id EntryID, clb func(*Entry)) error {
	return c.entries.RWithE(func(entries entries) error {
		return lookupErrClb(entries.entriesMap, id, clb, ErrEntryNotFound)
	})
}

func jobRunRClb(mx *mtx.RWMtx[jobRunsInner], runID RunID, clb func(*jobRunStruct)) error {
	return mx.RWithE(func(jobRunsIn jobRunsInner) error {
		return lookupErrClb(jobRunsIn.mapping, runID, clb, ErrJobRunNotFound)
	})
}

func (c *Cron) getHook(id HookID) (out Hook, err error) {
	err = c.hookRClb(id, func(hook hookMeta) { out = hook.hook.export(hook.evt, hook.entryID) })
	return out, err
}

func (c *Cron) getEntry(id EntryID) (out Entry, err error) {
	err = c.entryRClb(id, func(entry *Entry) { out = entry.snapshot() })
	return out, err
}

func (c *Cron) getJobRun(entryID EntryID, runID RunID) (out JobRun, err error) {
	err = c.getJobRunClb(entryID, runID, func(run *jobRunStruct) { out = run.export() })
	return out, err
}

func (c *Cron) enableHook(id HookID) {
	c.hookClb(id, func(hook hookMeta) { (*hook.hook).active = true })
}

func (c *Cron) disableHook(id HookID) {
	c.hookClb(id, func(hook hookMeta) { (*hook.hook).active = false })
}

func (c *Cron) setHookLabel(id HookID, label string) {
	c.hookClb(id, func(hook hookMeta) { (*hook.hook).label = label })
}

func (c *Cron) onJobStart(clb HookFn, opts ...HookOption) HookID {
	return c.onEvt(JobStart, clb, opts...)
}

func (c *Cron) onJobCompleted(clb HookFn, opts ...HookOption) HookID {
	return c.onEvt(JobCompleted, clb, opts...)
}

func (c *Cron) onEntryJobStart(entryID EntryID, clb HookFn, opts ...HookOption) HookID {
	return c.onEntryEvt(entryID, JobStart, clb, opts...)
}

func (c *Cron) onEntryJobCompleted(entryID EntryID, clb HookFn, opts ...HookOption) HookID {
	return c.onEntryEvt(entryID, JobCompleted, clb, opts...)
}

func (c *Cron) addJob(spec string, job IntoJob, opts ...EntryOption) (EntryID, error) {
	return c.addJobStrict(spec, castIntoJob(job), opts...)
}

func (c *Cron) addFunc(spec string, job FuncJob, opts ...EntryOption) (EntryID, error) {
	return c.addJobStrict(spec, job, opts...)
}

func (c *Cron) addJobStrict(spec string, job Job, opts ...EntryOption) (EntryID, error) {
	schedule, err := c.parseSpec(spec)
	if err != nil {
		var zeroID EntryID
		return zeroID, err
	}
	return c.schedule(&spec, schedule, job, opts...)
}

// parseSpec parses the spec and ensures the schedule will activate at least once
func (c *Cron) parseSpec(spec string) (Schedule, error) {
	schedule, err := c.parser.Parse(spec)
	if err != nil {
		return nil, err
	}
	if schedule.Next(c.now()).IsZero() {
		return nil, fmt.Errorf("%w: %s", ErrUnsatisfiableSchedule, spec)
	}
	return schedule, nil
}

func (c *Cron) schedule(spec *string, schedule Schedule, job Job, opts ...EntryOption) (EntryID, error) {
	if schedule == nil {
		return "", errors.New("schedule cannot be nil")
	}
	entry := Entry{
		ID:       EntryID(c.idFactory.Next()),
		job:      job,
		Spec:     spec,
		Schedule: schedule,
		Next:     schedule.Next(c.now()),
		Active:   true,
	}
	return c.addEntry(entry, opts...)
}

func (c *Cron) addEntry(entry Entry, opts ...EntryOption) (EntryID, error) {
	var zeroID EntryID
	for _, opt := range opts {
		opt(c, &entry)
	}
	if entry.ID == zeroID {
		return zeroID, errors.New("id cannot be empty")
	}
	if entry.job == nil {
		return zeroID, errors.New("job cannot be nil")
	}
	if entry.Schedule == nil {
		return zeroID, errors.New("schedule cannot be nil")
	}
	entry.Next = utils.TernaryOrZero(entry.Active, entry.Next)
	if err := c.entries.WithE(func(entries *entries) error {
		if _, ok := entries.entriesMap[entry.ID]; ok {
			return ErrIDAlreadyUsed
		}
		entries.entriesMap[entry.ID] = &entry
		entries.heap.Push(&entry)
		return nil
	}); err != nil {
		return zeroID, err
	}
	c.entriesUpdated() // addEntry
	return entry.ID, nil
}

func (c *Cron) entriesByTag(tag string) []Entry {
	return slices.DeleteFunc(c.getEntries(), func(e Entry) bool { return !e.HasTag(tag) })
}

// forEachTagged calls fn with the ID of each entry having the given tag (without holding the lock)
func (c *Cron) forEachTagged(tag string, fn func(EntryID)) int {
	var ids []EntryID
	c.entries.RWith(func(entries entries) {
		for id, entry := range entries.entriesMap {
			if entry.HasTag(tag) {
				ids = append(ids, id)
			}
		}
	})
	for _, id := range ids {
		fn(id)
	}
	return len(ids)
}

func (c *Cron) setTags(id EntryID, tags []string) error {
	return c.entries.WithE(func(entries *entries) error {
		return lookupErrClb(entries.entriesMap, id, func(entry *Entry) { entry.Tags = mergeTags(nil, tags) }, ErrEntryNotFound)
	})
}

func (c *Cron) updateLabel(id EntryID, label string) {
	c.entries.With(func(entries *entries) {
		if entry, ok := entries.entriesMap[id]; ok {
			(*entry).Label = label
		}
	})
}

func (c *Cron) modifyEntry(id EntryID, updateFunc func(entry *Entry) bool) error {
	if err := c.entries.WithE(func(entries *entries) error {
		entry, exists := entries.entriesMap[id]
		if !exists {
			return ErrEntryNotFound
		}
		if updateFunc != nil && !updateFunc(entry) {
			return errors.New("not changed")
		}
		now := c.now()
		newNext := utils.Ternary(updateFunc == nil, now, entry.Schedule.Next(now))
		newNext = utils.TernaryOrZero(entry.Active, newNext)
		(*entry).Next = newNext
		(*entry).pinnedNext = updateFunc == nil // RunNow must survive a Start
		_ = entries.heap.Update(id, newNext)
		return nil
	}); err != nil {
		return err
	}
	if updateFunc == nil {
		c.entriesUpdatedSync() // RunNow: the job is started when RunNow returns
	} else {
		c.entriesUpdated() // modifyEntry
	}
	return nil
}

func (c *Cron) runNow(id EntryID) error {
	if _, err := c.trigger(id); !errors.Is(err, ErrCronNotRunning) {
		return err
	}
	// Not running: run as soon as the cron is started
	return c.modifyEntry(id, nil)
}

func (c *Cron) trigger(id EntryID) (RunID, error) {
	if !c.isRunning() {
		return "", ErrCronNotRunning
	}
	var (
		ptr      *Entry // To check that the entry was not removed/replaced in the meantime
		snapshot Entry
	)
	if err := c.entries.WithE(func(entries *entries) error {
		entry, ok := entries.entriesMap[id]
		if !ok {
			return ErrEntryNotFound
		}
		entry.Prev = c.now()
		ptr, snapshot = entry, entry.snapshot()
		return nil
	}); err != nil {
		return "", err
	}
	// Create the job run without holding the lock, the factories are user code
	jobRun := newJobRun(c.lifecycle.Load().jobsCtx, c.clock, snapshot, c.idFactory, c.jobRunLoggerFactory, c.maxLogSize)
	jobRun.scheduledAt, jobRun.triggered = snapshot.Prev, true
	err := ErrEntryNotFound
	// Start the run while holding the read lock, so it cannot race with removeEntry
	c.entries.RWith(func(entries entries) {
		switch {
		case entries.entriesMap[id] != ptr:
		case snapshot.NoOverlap && c.entryIsRunning(id):
			err = ErrJobAlreadyRunning
		default:
			c.startJob(jobRun)
			err = nil
		}
	})
	if err != nil {
		jobRun.cancel()
		if errors.Is(err, ErrJobAlreadyRunning) {
			c.reportSkipped([]skippedActivation{{snapshot, jobRun.scheduledAt, SkipStillRunning}})
		}
		return "", err
	}
	return jobRun.runID, nil
}

func (c *Cron) setEntryActive(id EntryID, active bool) {
	_ = c.modifyEntry(id, func(entry *Entry) (updated bool) {
		if updated = entry.Active != active; updated {
			entry.Active = active
		}
		return
	})
}

func (c *Cron) updateSchedule(id EntryID, spec *string, schedule Schedule) error {
	return c.modifyEntry(id, func(entry *Entry) (updated bool) {
		if updated = entry.Spec != spec || spec == nil; updated {
			entry.Spec = spec
			entry.Schedule = schedule
		}
		return
	})
}

func (c *Cron) updateScheduleWithSpec(id EntryID, spec string) error {
	schedule, err := c.parseSpec(spec)
	if err != nil {
		return err
	}
	return c.updateSchedule(id, &spec, schedule)
}

func (c *Cron) getNextTime() (out time.Time) {
	c.entries.RWith(func(entries entries) {
		if e := entries.heap.Peek(); e != nil && e.Active {
			out = e.Next
		}
	})
	return
}

func (c *Cron) getNextDelay() (out time.Duration) {
	nextTime := c.getNextTime()
	if nextTime.IsZero() {
		return 100_000 * time.Hour // If there are no entries yet, just sleep - it still handles new entries and stop requests.
	} else {
		return nextTime.Sub(c.now())
	}
}

func (c *Cron) run() {
	ctx := c.ctx()
	timer := c.clock.NewTimer(c.getNextDelay()) // A single timer, reset at every iteration
	defer timer.Stop()
	for {
		var updated context.CancelFunc
		select {
		case <-timer.Chan():
		case <-c.notify:
		case updated = <-c.update:
		case <-ctx.Done():
			return
		}
		c.runDueEntries()
		if updated != nil {
			updated()
		}
		resetTimer(timer, c.getNextDelay())
	}
}

// resetTimer stops the timer, drains its channel if it already fired, and resets it
func resetTimer(timer clockwork.Timer, d time.Duration) {
	if !timer.Stop() {
		select {
		case <-timer.Chan():
		default:
		}
	}
	timer.Reset(d)
}

// entriesUpdated notifies the scheduler loop that the entries changed, without waiting for it
func (c *Cron) entriesUpdated() {
	if c.isRunning() { // If the cron is not running, no need to notify the main loop about updating entries
		utils.NonBlockingSend(c.notify, struct{}{}) // A pending notification is enough
	}
}

// entriesUpdatedSync notifies the scheduler loop that the entries changed,
// and waits until the due entries have been started.
func (c *Cron) entriesUpdatedSync() {
	if c.isRunning() { // If the cron is not running, no need to notify the main loop about updating entries
		ctx, cancel := context.WithCancel(c.ctx())
		select { // Wait until the main loop pickup our update task (or cron exiting)
		case c.update <- cancel:
		case <-ctx.Done():
		}
		<-ctx.Done() // Wait until "runDueEntries" is done running (or cron exiting)
	}
}

// Run every entry whose next time was less than now
func (c *Cron) runDueEntries() {
	if !c.isRunning() {
		return
	}
	now := c.now()
	type dueEntry struct {
		ptr      *Entry // To check that the entry was not removed/replaced in the meantime
		snapshot Entry
	}
	var (
		due     []dueEntry
		skipped []skippedActivation
	)
	c.entries.With(func(entries *entries) {
		for len(due) < maxRunsPerPass { // The remaining runs start at the next pass, right after this one
			entry := entries.heap.Peek()
			if entry == nil || entry.Next.After(now) || entry.Next.IsZero() || !entry.Active {
				break
			}
			scheduled, pinned := entry.Next, entry.pinnedNext
			entry.pinnedNext = false
			policy := c.misfirePolicyOf(entry)
			switch {
			case !pinned && policy == MisfireSkip && now.Sub(scheduled) > c.misfireThreshold:
				entry.Next = entry.Schedule.Next(now)
				entries.heap.Fix(0)
				skipped = append(skipped, skippedActivation{entry.snapshot(), scheduled, SkipMisfire})
				continue
			case !pinned && policy == MisfireRunAll:
				entry.Next = entry.Schedule.Next(scheduled) // The next missed activation, if any
			default:
				entry.Next = entry.Schedule.Next(now)
			}
			entry.Prev = scheduled
			entries.heap.Fix(0)
			due = append(due, dueEntry{entry, entry.snapshot()})
		}
	})
	c.reportSkipped(skipped)
	if len(due) == 0 {
		return
	}
	// Create the job runs without holding the lock, the factories are user code
	jobsCtx := c.lifecycle.Load().jobsCtx
	runs := make([]*jobRunStruct, len(due))
	for i, d := range due {
		runs[i] = newJobRun(jobsCtx, c.clock, d.snapshot, c.idFactory, c.jobRunLoggerFactory, c.maxLogSize)
		runs[i].scheduledAt = d.snapshot.Prev
	}
	// Start the runs while holding the read lock, so they cannot race with removeEntry
	skipped = skipped[:0]
	c.entries.RWith(func(entries entries) {
		for i, jobRun := range runs {
			switch {
			case entries.entriesMap[jobRun.entry.ID] != due[i].ptr:
				jobRun.cancel()
			case jobRun.entry.NoOverlap && c.entryIsRunning(jobRun.entry.ID):
				jobRun.cancel()
				skipped = append(skipped, skippedActivation{jobRun.entry, jobRun.scheduledAt, SkipStillRunning})
			default:
				c.startJob(jobRun)
			}
		}
	})
	c.reportSkipped(skipped)
}

func (c *Cron) getLocation() *time.Location {
	return c.location.Load()
}

func (c *Cron) setLocation(newLoc *time.Location) {
	c.location.Store(newLoc)
	c.setEntriesNext()
	c.entriesUpdated() // setLocation
}

// Reset all entries "Next" property.
// This is only done when the cron timezone is changed at runtime.
func (c *Cron) setEntriesNext() {
	now := c.now()
	c.entries.With(func(entries *entries) {
		for _, entry := range entries.heap.entries {
			entry.Next = utils.TernaryOrZero(entry.Active, entry.Schedule.Next(now))
			entry.pinnedNext = false
		}
		entries.heap.Init()
	})
}

// refreshStaleEntries recompute the "Next" of the entries that are in the past when the cron is started,
// so that jobs added long before calling Start (or before a Stop) do not all run immediately.
// Entries with an explicitly set "Next" (RunOnStart, WithNext, RunNow) are left untouched.
//
// What happens to the activations missed while the cron was not running depends on the misfire policy:
// with MisfireDefault they are skipped, with MisfireSkip only those missed by more than the misfire
// threshold are, and with MisfireRunOnce / MisfireRunAll they are left to the scheduler loop.
func (c *Cron) refreshStaleEntries() {
	now := c.now()
	var skipped []skippedActivation
	c.entries.With(func(entries *entries) {
		for _, entry := range entries.heap.entries {
			if !entry.Active || entry.pinnedNext || entry.Next.IsZero() || !entry.Next.Before(now) {
				continue
			}
			switch c.misfirePolicyOf(entry) {
			case MisfireRunOnce, MisfireRunAll:
				continue
			case MisfireSkip:
				if now.Sub(entry.Next) <= c.misfireThreshold {
					continue
				}
			}
			skipped = append(skipped, skippedActivation{entry.snapshot(), entry.Next, SkipMisfire})
			entry.Next = entry.Schedule.Next(now)
		}
		entries.heap.Init()
	})
	c.reportSkipped(skipped)
}

func (c *Cron) remove(id EntryID) {
	c.removeKeepRun(id, "")
}

// removeKeepRun removes an entry and cancels all of its running jobs, except the run "keepRunID".
// This is used by jobs that remove their own entry (Once, N) without cancelling themselves.
func (c *Cron) removeKeepRun(id EntryID, keepRunID RunID) {
	c.removeEntry(id, keepRunID)
	c.entriesUpdated() // remove
}

func (c *Cron) removeEntry(id EntryID, keepRunID RunID) {
	// Job runs are registered while holding the entries lock (see runDueEntries),
	// so no new run can be registered for this entry once we are done here.
	c.entries.With(func(entries *entries) {
		delete(entries.entriesMap, id)
		entries.heap.Remove(id)
		if jobRuns, ok := c.runningJobsMap.LoadAndDelete(id); ok {
			jobRuns.With(func(v *jobRunsInner) {
				for _, j := range v.running {
					if j.runID != keepRunID {
						j.cancel()
					}
				}
			})
		}
	})
	c.hooks.With(func(v *hooksContainer) { v.removeEntryHooks(id) })
}

func (c *Cron) getEntries() (out []Entry) {
	c.entries.RWith(func(entries entries) {
		out = entries.heap.Entries()
	})
	return
}

func (c *Cron) getJobRunClb(entryID EntryID, runID RunID, clb func(*jobRunStruct)) error {
	jobRunsInnerMtx, ok := c.runningJobsMap.Load(entryID)
	if !ok {
		return ErrEntryNotFound
	}
	return jobRunRClb(jobRunsInnerMtx, runID, clb)
}

func (c *Cron) cancelRun(entryID EntryID, runID RunID) error {
	return c.getJobRunClb(entryID, runID, func(run *jobRunStruct) {
		(*run).cancel()
	})
}

func (c *Cron) entryIsRunning(entryID EntryID) (isRunning bool) {
	jobRuns, ok := c.runningJobsMap.Load(entryID)
	return ok && jobRunsClb(jobRuns, func(v jobRunsInner) bool { return len(v.running) > 0 })
}

func exportJobRuns(runs []*jobRunStruct) (out []JobRun) {
	for _, j := range runs {
		out = append(out, j.export())
	}
	return
}

func jobRunsClb[T any](jobRuns *mtx.RWMtx[jobRunsInner], clb func(jobRunsInner) T) (out T) {
	jobRuns.RWith(func(v jobRunsInner) { out = clb(v) })
	return
}

// jobRunsExportClb exports the job runs while holding the lock,
// the slices returned by "clb" are modified in place (slices.Delete) once the lock is released.
func jobRunsExportClb(jobRuns *mtx.RWMtx[jobRunsInner], clb func(jobRunsInner) []*jobRunStruct) []JobRun {
	return jobRunsClb(jobRuns, func(v jobRunsInner) []JobRun { return exportJobRuns(clb(v)) })
}

func (c *Cron) jobRunsClb(clb func(jobRunsInner) []*jobRunStruct) (out []JobRun) {
	for jobRuns := range c.runningJobsMap.IterValues() {
		out = append(out, jobRunsExportClb(jobRuns, clb)...)
	}
	sortJobRunsPublic(out)
	return
}

func (c *Cron) jobRunsForClb(entryID EntryID, clb func(jobRunsInner) []*jobRunStruct) (out []JobRun, err error) {
	if jobRuns, ok := c.runningJobsMap.Load(entryID); ok {
		out = jobRunsExportClb(jobRuns, clb)
		return
	}
	return nil, ErrEntryNotFound
}

var getRunningSliceClb = func(v jobRunsInner) []*jobRunStruct { return v.running }
var getCompletedSliceClb = func(v jobRunsInner) []*jobRunStruct { return v.completed }

func (c *Cron) runningJobs() (out []JobRun) {
	return c.jobRunsClb(getRunningSliceClb)
}

func (c *Cron) completedJobs() (out []JobRun) {
	return c.jobRunsClb(getCompletedSliceClb)
}

func (c *Cron) recentCompletedJobs(limit int) []JobRun {
	var runs []*jobRunStruct
	for jobRuns := range c.runningJobsMap.IterValues() {
		jobRuns.RWith(func(v jobRunsInner) { runs = append(runs, v.completed...) }) // Copy the pointers under the lock
	}
	slices.SortFunc(runs, func(a, b *jobRunStruct) int { return b.createdAt.Compare(a.createdAt) })
	if limit > 0 && len(runs) > limit {
		runs = runs[:limit]
	}
	return exportJobRuns(runs)
}

func (c *Cron) runningJobsFor(entryID EntryID) (out []JobRun, err error) {
	return c.jobRunsForClb(entryID, getRunningSliceClb)
}

func (c *Cron) completedJobRunsFor(entryID EntryID) (out []JobRun, err error) {
	return c.jobRunsForClb(entryID, getCompletedSliceClb)
}

func sortJobRunsPublic(runs []JobRun) {
	sort.Slice(runs, func(i, j int) bool {
		return runs[i].CreatedAt.Before(runs[j].CreatedAt)
	})
}

// registerJobRun adds the job run to the running jobs of the entry.
// Must be called while holding the entries lock, so it cannot race with removeEntry.
func (c *Cron) registerJobRun(entryID EntryID, jobRun *jobRunStruct) {
	jobRuns, _ := c.runningJobsMap.LoadOrStore(entryID, utils.Ptr(mtx.NewRWMtx(jobRunsInner{mapping: make(map[RunID]*jobRunStruct)})))
	jobRuns.With(func(v *jobRunsInner) {
		(*v).mapping[jobRun.runID] = jobRun
		(*v).running = append((*v).running, jobRun)
	})
}

// completeJobRun moves the job run from the running jobs to the completed jobs of the entry.
func (c *Cron) completeJobRun(entryID EntryID, jobRun *jobRunStruct) {
	utils.NonBlockingSend(c.jobRunCompletedCh, jobRun.export())
	jobRuns, ok := c.runningJobsMap.Load(entryID)
	if !ok { // Entry was removed while the job was running
		return
	}
	jobRuns.With(func(v *jobRunsInner) {
		if idx := slices.Index((*v).running, jobRun); idx != -1 {
			(*v).running = slices.Delete((*v).running, idx, idx+1)
			if c.keepCompletedRunsDur.Get() > 0 || c.maxCompletedRuns > 0 {
				completedAt := jobRun.inner.Get().completedAt
				(*v).completed = append((*v).completed, jobRun)
				(*v).completedTS = append((*v).completedTS, *completedAt)
				if c.maxCompletedRuns > 0 && len((*v).completed) > c.maxCompletedRuns {
					freeOldestCompletedRuns(v, len((*v).completed)-c.maxCompletedRuns)
				}
			} else {
				freeJobRun(v, jobRun)
			}
		}
	})
}

func freeJobRun(v *jobRunsInner, jobRun *jobRunStruct) {
	delete((*v).mapping, jobRun.runID)
}

// freeOldestCompletedRuns frees the "n" oldest completed job runs
func freeOldestCompletedRuns(v *jobRunsInner, n int) {
	for _, jobRun := range (*v).completed[:n] {
		freeJobRun(v, jobRun)
	}
	// slices.Delete clears the tail, so the backing array does not retain the freed runs
	(*v).completed = slices.Delete((*v).completed, 0, n)
	(*v).completedTS = slices.Delete((*v).completedTS, 0, n)
}

// startJob runs the given job in a new goroutine.
// Must be called while holding the entries lock.
func (c *Cron) startJob(jobRun *jobRunStruct) {
	entryID := jobRun.entry.ID
	c.runningJobsCount.Add(1)
	c.registerJobRun(entryID, jobRun)
	go func() {
		defer func() {
			jobRun.cancel() // Release the job context as soon as the job is done
			c.completeJobRun(entryID, jobRun)
			c.runningJobsCount.Add(-1)
			c.signalJobCompleted()
		}()
		utils.NonBlockingSend(c.jobRunCreatedCh, jobRun.export())
		release, err := c.limits.acquire(jobRun.ctx, jobRun.entry.Tags)
		if err != nil { // Cancelled while waiting for a concurrency limit
			c.cancelledBeforeStart(jobRun, err)
			return
		}
		defer release()
		c.runWithRecovery(jobRun)
	}()
}

// cancelledBeforeStart completes a job run cancelled before the job started
func (c *Cron) cancelledBeforeStart(jobRun *jobRunStruct, err error) {
	c.logger.Info("job cancelled before starting", "label", jobRun.entry.Label, "entryID", jobRun.entry.ID, "runID", jobRun.runID, "error", err)
	makeEventErr(c, jobRun, JobErr, err)
	makeEvent(c, jobRun, JobCompleted)
	c.withMetrics(func(m Metrics) { m.JobCompleted(jobRun.export(), 0) })
}

func (c *Cron) runWithRecovery(jobRun *jobRunStruct) {
	runID, entry := jobRun.runID, jobRun.entry
	entryID, label := entry.ID, entry.Label
	clock, logger := c.clock, c.logger
	start := clock.Now()
	defer func() {
		if r := recover(); r != nil {
			logger.Error("job panic", "label", label, "entryID", entryID, "runID", runID, "error", r, "stack", string(debug.Stack()))
			makeEvent(c, jobRun, JobPanic)
		}
		duration := clock.Since(start)
		logger.Info("job completed", "label", label, "entryID", entryID, "runID", runID, "duration", duration)
		makeEvent(c, jobRun, JobCompleted)
		c.withMetrics(func(m Metrics) { m.JobCompleted(jobRun.export(), duration) })
	}()
	logger.Info("job start", "label", label, "entryID", entryID, "runID", runID)
	makeEvent(c, jobRun, JobStart)
	c.withMetrics(func(m Metrics) { m.JobStarted(jobRun.export(), start.Sub(jobRun.scheduledAt)) })
	if err := entry.job.Run(jobRun.ctx, c, jobRun.export()); err != nil {
		logger.Error("job error", "label", label, "entryID", entryID, "runID", runID, "error", err)
		makeEventErr(c, jobRun, JobErr, err)
	}
}

func makeEvent(c *Cron, jobRun *jobRunStruct, typ JobEventType) {
	makeEventErr(c, jobRun, typ, nil)
}

func makeEventErr(c *Cron, jobRun *jobRunStruct, typ JobEventType, err error) {
	clock := c.clock
	now := clock.Now()
	var opt func(*jobRunInner)
	switch typ {
	case JobStart:
		opt = func(inner *jobRunInner) { (*inner).startedAt = &now }
	case JobCompleted:
		opt = func(inner *jobRunInner) { (*inner).completedAt = &now }
	case JobPanic:
		opt = func(inner *jobRunInner) { (*inner).panic = true }
	case JobErr:
		opt = func(inner *jobRunInner) { (*inner).error = err }
	}
	evt := newJobEvent(typ, clock)
	jobRun.inner.With(func(inner *jobRunInner) {
		utils.ApplyOptions(inner, opt)
		inner.addEvent(evt)
	})
	c.ps.Pub(jobRun.entry.ID, evt)
	// Hooks receive the cron context, the job context is cancelled as soon as the job completes,
	// which would break async hooks.
	triggerHooks(c, jobRun.cronCtx, jobRun, typ)
}

func triggerHooks(c *Cron, ctx context.Context, jobRun *jobRunStruct, evtType JobEventType) {
	// Collect the hooks while holding the lock, but run them after releasing it,
	// so that sync hooks can use the hooks API (RemoveHook, OnEvt...) without deadlocking.
	var hooks []*hookStruct
	c.hooks.RWith(func(v hooksContainer) {
		for _, hook := range v.hooksFor(evtType, jobRun.entry.ID) {
			if hook.active {
				hooks = append(hooks, hook)
			}
		}
	})
	if len(hooks) == 0 {
		return
	}
	jr := jobRun.export() // Only export when needed
	utils.ForEach(hooks, func(hook *hookStruct) {
		fn := func() { hook.fn(ctx, c, hook.id, jr) }
		maybeAsync(fn, hook.runAsync)()
	})
}

func (c *Cron) signalJobCompleted() {
	c.cond.L.Lock()
	c.cond.Broadcast()
	c.cond.L.Unlock()
}

func (c *Cron) waitAllJobsCompleted() {
	c.cond.L.Lock()
	for c.runningJobsCount.Load() != 0 {
		c.cond.Wait()
	}
	c.cond.L.Unlock()
}

// now returns current time in c location
func (c *Cron) now() time.Time {
	return c.clock.Now().In(c.getLocation())
}
