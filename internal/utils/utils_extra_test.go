package utils

import (
	mathRand "math/rand/v2"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFindIdx(t *testing.T) {
	el, idx := FindIdx([]int{1, 2, 3}, func(v int) bool { return v == 2 })
	assert.Equal(t, 2, *el)
	assert.Equal(t, 1, idx)
	el, idx = FindIdx([]int{1, 2, 3}, func(v int) bool { return v == 4 })
	assert.Nil(t, el)
	assert.Equal(t, -1, idx)
}

func TestForEach(t *testing.T) {
	var sum int
	ForEach([]int{1, 2, 3}, func(v int) { sum += v })
	assert.Equal(t, 6, sum)
}

func TestNonBlockingSend(t *testing.T) {
	ch := make(chan int, 1)
	NonBlockingSend(ch, 1)
	NonBlockingSend(ch, 2) // full: dropped, does not block
	assert.Equal(t, 1, <-ch)
	NonBlockingSend(make(chan int), 1) // no receiver: does not block
}

func TestUuidV4Str(t *testing.T) {
	re := regexp.MustCompile(`^[0-9a-f]{12}4[0-9a-f]{3}[89ab][0-9a-f]{15}$`) // version 4, variant 10
	seen := map[string]bool{}
	for i := 0; i < 100; i++ {
		id := UuidV4Str()
		assert.Regexp(t, re, id)
		assert.False(t, seen[id])
		seen[id] = true
	}
}

func TestRandomFromSameBounds(t *testing.T) {
	r := mathRand.New(mathRand.NewPCG(1, 2))
	assert.Equal(t, int64(5), RandomFrom(r, 5, 5))
	for i := 0; i < 100; i++ {
		v := RandomFrom(r, 10, 1) // bounds in any order
		assert.GreaterOrEqual(t, v, int64(1))
		assert.LessOrEqual(t, v, int64(10))
	}
}
