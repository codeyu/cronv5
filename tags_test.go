package cron

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTags(t *testing.T) {
	cron, _ := newRegressionCron()
	a, _ := cron.AddJob("* * * * * *", func() {}, WithID("a"), WithTags("billing", "nightly"))
	b, _ := cron.AddJob("* * * * * *", func() {}, WithID("b"), WithTags("billing"), WithTags("billing", "", "eu"))
	_, _ = cron.AddJob("* * * * * *", func() {}, WithID("c"))

	entry, _ := cron.Entry(a)
	assert.Equal(t, []string{"billing", "nightly"}, entry.Tags)
	assert.True(t, entry.HasTag("nightly"))
	assert.False(t, entry.HasTag("eu"))
	entry, _ = cron.Entry(b)
	assert.Equal(t, []string{"billing", "eu"}, entry.Tags) // no duplicated or empty tags

	ids := func(entries []Entry) (out []EntryID) {
		for _, e := range entries {
			out = append(out, e.ID)
		}
		return
	}
	assert.ElementsMatch(t, []EntryID{"a", "b"}, ids(cron.EntriesByTag("billing")))
	assert.Equal(t, []EntryID{"a"}, ids(cron.EntriesByTag("nightly")))
	assert.Empty(t, cron.EntriesByTag("unknown"))

	// Snapshots do not share memory with the entries
	entry.Tags[0] = "changed"
	entry, _ = cron.Entry(b)
	assert.Equal(t, "billing", entry.Tags[0])
	cron.Entries()[0].Tags[0] = "changed"
	assert.Len(t, cron.EntriesByTag("billing"), 2)

	assert.NoError(t, cron.SetTags(b, "eu"))
	assert.Equal(t, []EntryID{"a"}, ids(cron.EntriesByTag("billing")))
	assert.ErrorIs(t, cron.SetTags("unknown", "x"), ErrEntryNotFound)
}

func TestTagsEnableDisableRemove(t *testing.T) {
	cron, _ := newRegressionCron()
	cron.Start()
	defer cron.Stop()
	var calls atomic.Int32
	for _, id := range []EntryID{"a", "b"} {
		_, _ = cron.AddJob("* * * * * *", func() { calls.Add(1) }, WithID(id), WithTags("group"))
	}
	_, _ = cron.AddJob("* * * * * *", func() {}, WithID("other"))

	assert.Equal(t, 2, cron.DisableByTag("group"))
	advanceAndCycle(cron, time.Second)
	assert.Equal(t, int32(0), calls.Load())
	for _, e := range cron.EntriesByTag("group") {
		assert.False(t, e.Active)
	}
	other, _ := cron.Entry("other")
	assert.True(t, other.Active)

	assert.Equal(t, 2, cron.EnableByTag("group"))
	advanceAndCycle(cron, time.Second)
	assert.Equal(t, int32(2), calls.Load())

	assert.Equal(t, 2, cron.RemoveByTag("group"))
	assert.Equal(t, 0, cron.RemoveByTag("group"))
	assert.Len(t, cron.Entries(), 1)
}

func TestTagsInJobRun(t *testing.T) {
	cron, _ := newRegressionCron()
	cron.Start()
	defer cron.Stop()
	tags := make(chan []string, 1)
	id, _ := cron.AddJob("0 0 0 1 1 *", func(_ context.Context, _ *Cron, r JobRun) { tags <- r.Entry.Tags }, WithTags("x", "y"))
	_, err := cron.Trigger(id)
	assert.NoError(t, err)
	assert.Equal(t, []string{"x", "y"}, <-tags)
}
