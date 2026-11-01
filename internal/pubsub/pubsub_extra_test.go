package pubsub

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestReceiveCh(t *testing.T) {
	ps := NewPubSub[string, int]()
	s := ps.Subscribe([]string{"a"})
	defer s.Close()
	ps.Pub("a", 1)
	select {
	case p := <-s.ReceiveCh():
		assert.Equal(t, Payload[string, int]{"a", 1}, p)
	case <-time.After(time.Second):
		t.Fatal("no message")
	}
}

func TestReceiveTimeout2Cancelled(t *testing.T) {
	ps := NewPubSub[string, int]()
	s := ps.Subscribe([]string{"a"})
	stop := make(chan struct{})
	close(stop)
	_, _, err := s.ReceiveTimeout2(time.Second, stop)
	assert.ErrorIs(t, err, ErrCancelled)

	// Closed subscriber
	s.Close()
	_, _, err = s.ReceiveTimeout2(time.Second, make(chan struct{}))
	assert.ErrorIs(t, err, ErrCancelled)
}
