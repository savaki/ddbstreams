package shardstate

import (
	"testing"
	"time"

	"github.com/tj/assert"
)

func TestRegistry(t *testing.T) {
	t.Run("MarkProcessing", func(t *testing.T) {
		shardID := "abc"
		registry := New(time.Hour)
		defer registry.Close()

		assert.True(t, registry.MarkProcessing(shardID))  // 1st - ok
		assert.False(t, registry.MarkProcessing(shardID)) // 2nd - fails; already processing
	})

	t.Run("MarkProcessing - on completed record", func(t *testing.T) {
		shardID := "abc"
		registry := New(time.Hour)
		defer registry.Close()

		registry.MarkCompleted(shardID)
		assert.False(t, registry.MarkProcessing(shardID)) // fails; already completed
	})

	t.Run("FindState", func(t *testing.T) {
		shardID := "abc"
		registry := New(time.Hour)
		defer registry.Close()

		_, ok := registry.FindState(shardID)
		assert.False(t, ok)

		registry.MarkProcessing(shardID)
		state, ok := registry.FindState(shardID)
		assert.True(t, ok)
		assert.Equal(t, Processing, state)

		registry.MarkCompleted(shardID)
		state, ok = registry.FindState(shardID)
		assert.True(t, ok)
		assert.Equal(t, Completed, state)
	})

	t.Run("Flush", func(t *testing.T) {
		shardID := "abc"
		registry := New(time.Hour)
		defer registry.Close()

		registry.MarkProcessing(shardID)

		n := registry.Flush(time.Now().Add(defaultFlushInterval + time.Hour))
		assert.Equal(t, 1, n)

		_, ok := registry.FindState(shardID)
		assert.False(t, ok)
	})
}
