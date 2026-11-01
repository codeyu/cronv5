/*
Package cron implements a cron spec parser and job runner.

# Usage

Callers may register jobs to be invoked on a given schedule. Cron will run
them in their own goroutines.

	c := cron.New().Build()
	_, _ = c.AddJob("30 * * * *", func() { fmt.Println("Every hour on the half hour") })
	_, _ = c.AddJob("@hourly", func() { fmt.Println("Every hour") })
	_, _ = c.AddJob("@every 1h30m", func() { fmt.Println("Every hour thirty") })
	c.Start()
	..
	// Jobs are invoked in their own goroutine, asynchronously.
	...
	// Jobs may also be added to a running Cron
	_, _ = c.AddJob("@daily", func() { fmt.Println("Every day") })
	..
	// Inspect the cron job entries' next and previous run times.
	inspect(c.Entries())
	..
	<-c.Stop() // Stop the scheduler, cancel the context of the running jobs, and wait for them to complete.

AddJob accepts any function (or any type with a "Run" method) with a signature
supported by J, for example: func(), func() error, func(context.Context) error,
func(context.Context, cron.JobRun) error... AddFunc and AddJobStrict only accept
the complete Job signature: func(context.Context, *cron.Cron, cron.JobRun) error.

Each run of a job receives a context that is cancelled when the run is cancelled
(CancelJobRun), when the entry is removed, when the cron is stopped, or when the job completes.

# Stopping

Stop stops the scheduler and returns a channel that is closed once all running jobs completed.
By default, Stop cancels the context of the running jobs. Use the KeepJobsRunningOnStop
builder option to let them complete, or Shutdown(ctx) for a graceful stop with a deadline:

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := c.Shutdown(ctx); err != nil {
		// Some jobs were still running after 30s, their context has been cancelled.
	}

A stopped cron can be started again. Entries whose next activation time went by while the cron
was not running are rescheduled, instead of all running at once when the cron starts.

# CRON Expression Format

A cron expression represents a set of times, using 5 space-separated fields.
When the cron is built with WithSeconds, a mandatory seconds field is added in first position,
and the day of week field becomes optional.

	Field name   | Mandatory? | Allowed values  | Allowed special characters
	----------   | ---------- | --------------  | --------------------------
	Seconds      | WithSeconds| 0-59            | * / , -
	Minutes      | Yes        | 0-59            | * / , -
	Hours        | Yes        | 0-23            | * / , -
	Day of month | Yes        | 1-31            | * / , - ? L W
	Month        | Yes        | 1-12 or JAN-DEC | * / , -
	Day of week  | Yes        | 0-7 or SUN-SAT  | * / , - ? L #

Day of week 0 and 7 are both Sunday (Vixie cron numbering; note that Quartz uses 1 for Sunday).

Note: Month and Day-of-week field values are case insensitive. "SUN", "Sun",
and "sun" are equally accepted.

Specs that never activate (eg: "0 0 30 2 *", February 30th) are rejected with ErrUnsatisfiableSchedule.

A custom parser can be configured with NewParser and the WithParser builder option.

# Special Characters

Asterisk ( * )

The asterisk indicates that the cron expression will match for all values of the
field; e.g., using an asterisk in the 4th field (month) would indicate every
month.

Slash ( / )

Slashes are used to describe increments of ranges. For example 3-59/15 in the
1st field (minutes) would indicate the 3rd minute of the hour and every 15
minutes thereafter. The form "*\/..." is equivalent to the form "first-last/...",
that is, an increment over the largest possible range of the field. The form
"N/..." is accepted as meaning "N-MAX/...", that is, starting at N, use the
increment until the end of that specific range. It does not wrap around.

Comma ( , )

Commas are used to separate items of a list. For example, using "MON,WED,FRI" in
the 5th field (day of week) would mean Mondays, Wednesdays and Fridays.

Hyphen ( - )

Hyphens are used to define ranges. For example, 9-17 would indicate every
hour between 9am and 5pm inclusive.

In the month and day of week fields, a range can wrap around: "FRI-MON" is Friday,
Saturday, Sunday and Monday, "NOV-FEB" is November to February. The other fields
reject a range whose beginning is after its end.

Question mark ( ? )

Question mark may be used instead of '*' for leaving either day-of-month or
day-of-week blank.

L, W and # (day of month and day of week)

These items depend on the month, they can be combined with other items in a list
(eg: "1,15,L"), but not in a range or with an increment.

	Item   | Field        | Meaning
	----   | -----        | -------
	L      | Day of month | Last day of the month
	L-n    | Day of month | n days before the last day of the month (0-30), eg: L-2
	LW     | Day of month | Last weekday (Monday to Friday) of the month
	nW     | Day of month | Weekday nearest to the n-th day of the month, eg: 15W
	dL     | Day of week  | Last day d of the month, eg: 5L or FRIL (last Friday)
	d#k    | Day of week  | k-th day d of the month (1-5), eg: MON#1 (first Monday)

"nW" never leaves the month: when the 1st is a Saturday, "1W" is Monday the 3rd, and
when the 31st is a Sunday, "31W" is Friday the 29th. It does not activate in months
that have no n-th day (eg: "31W" in April).

"L" alone in the day of week field is rejected, as its meaning differs between cron
implementations: use 6 (or SAT) for Saturday, or 5L for the last Friday of the month.

As for other items, when both the day of month and day of week fields are restricted
(neither is '*' or '?'), the schedule activates on days matching either one:
"0 0 L * MON#1" activates on the first Monday and on the last day of every month.

# Predefined schedules

You may use one of several pre-defined schedules in place of a cron expression.

	Entry                  | Description                                | Equivalent To
	-----                  | -----------                                | -------------
	@yearly (or @annually) | Run once a year, midnight, Jan. 1st        | 0 0 1 1 *
	@monthly               | Run once a month, midnight, first of month | 0 0 1 * *
	@weekly                | Run once a week, midnight between Sat/Sun  | 0 0 * * 0
	@daily (or @midnight)  | Run once a day, midnight                   | 0 0 * * *
	@hourly                | Run once an hour, beginning of hour        | 0 * * * *

# Activation times

The activation times of a schedule can be listed without running it:

	sched, _ := cron.ParseStandard("0 9 * * MON-FRI")
	next5 := cron.NextN(sched, time.Now(), 5) // the next 5 activation times
	for t := range cron.Activations(sched, time.Now()) {
		// successive activation times, until break
	}
	prev := sched.(*cron.SpecSchedule).Prev(time.Now()) // the last activation time

For an entry of a Cron, NextActivations lists the next activation times in the cron location,
starting with the entry's next activation time:

	times, err := c.NextActivations(id, 5)

# Intervals

You may also schedule a job to execute at fixed intervals, starting at the time it's added
or cron is run. This is supported by formatting the cron spec like this:

	@every <duration>

where "duration" is a string accepted by time.ParseDuration
(http://golang.org/pkg/time/#ParseDuration).

For example, "@every 1h30m10s" would indicate a schedule that activates after
1 hour, 30 minutes, 10 seconds, and then every interval after that.

Note: The interval does not take the job runtime into account. For example,
if a job takes 3 minutes to run, and it is scheduled to run every 5 minutes,
it will have only 2 minutes of idle time between each run.

# Time zones

By default, all interpretation and scheduling is done in the time zone of the clock
(the machine's local time zone). It can be changed with the WithLocation builder option,
or at runtime with SetLocation.

A spec can also be prefixed with "CRON_TZ=<zone>" (or "TZ=<zone>") to use a specific time zone,
for example "CRON_TZ=Asia/Tokyo 0 6 * * *".

The tz database of the machine is used (see time.LoadLocation). To embed it in the program
instead, import the time/tzdata package:

	import _ "time/tzdata"

# Daylight saving time

Daylight saving time transitions are handled as Vixie cron (the Linux crontab) does. Schedules
whose second, minute or hour field starts with '*' (eg: "*\/5 * * * *", "0 * * * *") are
interval schedules, the other ones (eg: "30 2 * * *", "0 9 * * MON") run at a fixed time.

When the clock jumps forward (eg: from 2:00 to 3:00):
  - a fixed time schedule whose time is skipped runs once at the end of the jump (3:00);
  - an interval schedule skips the times that do not exist.

When the clock goes back (eg: from 2:00 to 1:00, 1:00-2:00 occurs twice):
  - a fixed time schedule whose time is repeated runs only the first time;
  - an interval schedule runs both times.

So "30 2 * * *" runs every day, once, and "0 * * * *" runs every hour of elapsed time.
Time zones without daylight saving time (eg: UTC, Asia/Tokyo) are not affected.

# Thread safety

Since the Cron service runs concurrently with the calling code, some amount of
care must be taken to ensure proper synchronization.

All cron methods are designed to be correctly synchronized as long as the caller
ensures that invocations have a clear happens-before ordering between them.

# Implementation

Cron entries are stored in a min-heap, ordered by their next activation time. Cron
sleeps until the next job is due to be run.

Upon waking:
  - it runs each entry that is active on that second
  - it calculates the next run times for the jobs that were run
  - it updates the position of these entries in the heap
  - it goes to sleep until the soonest job.
*/
package cron
