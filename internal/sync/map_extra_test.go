package sync

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLoadOrStore(t *testing.T) {
	m := Map[string, int]{}
	v, loaded := m.LoadOrStore("a", 1)
	assert.Equal(t, 1, v)
	assert.False(t, loaded)
	v, loaded = m.LoadOrStore("a", 2)
	assert.Equal(t, 1, v)
	assert.True(t, loaded)
}

func TestLoadAndDelete(t *testing.T) {
	m := Map[string, int]{}
	v, loaded := m.LoadAndDelete("a")
	assert.Equal(t, 0, v)
	assert.False(t, loaded)
	m.Store("a", 1)
	v, loaded = m.LoadAndDelete("a")
	assert.Equal(t, 1, v)
	assert.True(t, loaded)
	assert.False(t, m.Has("a"))
}

func TestSwap(t *testing.T) {
	m := Map[string, int]{}
	v, loaded := m.Swap("a", 1)
	assert.Equal(t, 0, v)
	assert.False(t, loaded)
	v, loaded = m.Swap("a", 2)
	assert.Equal(t, 1, v)
	assert.True(t, loaded)
	v, _ = m.Load("a")
	assert.Equal(t, 2, v)
}

func TestIterAndClear(t *testing.T) {
	m := Map[string, int]{}
	m.Store("a", 1)
	m.Store("b", 2)
	keys := slices.Sorted(m.IterKeys())
	assert.Equal(t, []string{"a", "b"}, keys)
	values := slices.Sorted(m.IterValues())
	assert.Equal(t, []int{1, 2}, values)

	// Stopping the iteration early
	n := 0
	for range m.IterValues() {
		n++
		break
	}
	assert.Equal(t, 1, n)

	m.Clear()
	assert.False(t, m.Has("a"))
	assert.Empty(t, slices.Collect(m.IterKeys()))
}
