package cron

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestEntryMethodsReturnErrEntryNotFound(t *testing.T) {
	cron, _ := newRegressionCron()
	id, _ := cron.AddJob("* * * * * *", func() {})

	assert.NoError(t, cron.Disable(id))
	assert.NoError(t, cron.Disable(id)) // already disabled: no change, no error
	entry, _ := cron.Entry(id)
	assert.False(t, entry.Active)
	assert.NoError(t, cron.Enable(id))
	assert.NoError(t, cron.Enable(id))
	entry, _ = cron.Entry(id)
	assert.True(t, entry.Active)

	assert.NoError(t, cron.UpdateLabel(id, "label"))
	entry, _ = cron.Entry(id)
	assert.Equal(t, "label", entry.Label)

	for _, err := range []error{
		cron.Enable("unknown"),
		cron.Disable("unknown"),
		cron.UpdateLabel("unknown", "x"),
		cron.Remove("unknown"),
	} {
		assert.ErrorIs(t, err, ErrEntryNotFound)
	}

	assert.NoError(t, cron.Remove(id))
	assert.ErrorIs(t, cron.Remove(id), ErrEntryNotFound)
}

func TestHookMethodsReturnErrHookNotFound(t *testing.T) {
	cron, _ := newRegressionCron()
	id, _ := cron.AddJob("* * * * * *", func() {})
	noop := func(context.Context, *Cron, HookID, JobRun) {}
	global := cron.OnJobStart(noop)
	entryHook := cron.OnEntryJobCompleted(id, noop)

	for _, hookID := range []HookID{global, entryHook} {
		assert.NoError(t, cron.DisableHook(hookID))
		hook, _ := cron.GetHook(hookID)
		assert.False(t, hook.Active)
		assert.NoError(t, cron.EnableHook(hookID))
		assert.NoError(t, cron.SetHookLabel(hookID, "label"))
		hook, _ = cron.GetHook(hookID)
		assert.True(t, hook.Active)
		assert.Equal(t, "label", hook.Label)
		assert.NoError(t, cron.RemoveHook(hookID))
		assert.ErrorIs(t, cron.RemoveHook(hookID), ErrHookNotFound)
	}
	for _, err := range []error{
		cron.EnableHook("unknown"),
		cron.DisableHook("unknown"),
		cron.SetHookLabel("unknown", "x"),
		cron.RemoveHook("unknown"),
	} {
		assert.ErrorIs(t, err, ErrHookNotFound)
	}
}

// The jobs removing their own entry (Once, N) are not affected by the entry being removed first
func TestOnceAfterRemove(t *testing.T) {
	cron, _ := newRegressionCron()
	cron.Start()
	defer cron.Stop()
	ran := make(chan struct{}, 1)
	id, _ := cron.AddJob("0 0 0 1 1 *", Once(func() { ran <- struct{}{} }))
	_, err := cron.Trigger(id)
	assert.NoError(t, err)
	<-ran
	cron.waitAllJobsCompleted()
	assert.ErrorIs(t, cron.Remove(id), ErrEntryNotFound) // already removed by Once
}

// The entry options are EntryOption values
func TestEntryOptionTypes(t *testing.T) {
	opts := []EntryOption{Label("x"), WithID("id"), WithNext(time.Now()), RunOnStart, Disabled, NoOverlap, WithTags("t"), WithMisfirePolicy(MisfireSkip)}
	assert.Len(t, opts, 8)
}
