package shardstate

import (
	"testing"
	"time"
)

func TestRegistry(t *testing.T) {
	t.Run("MarkProcessing", func(t *testing.T) {
		shardID := "abc"
		registry := New(time.Hour)
		defer registry.Close()

		// 1st - ok
		if ok := registry.MarkProcessing(shardID); !ok {
			t.Fatalf("got false; want true")
		}

		// 2nd - fails; already processing
		if ok := registry.MarkProcessing(shardID); ok {
			t.Fatalf("got true; want false")
		}
	})

	t.Run("MarkProcessing - on completed record", func(t *testing.T) {
		shardID := "abc"
		registry := New(time.Hour)
		defer registry.Close()

		registry.MarkCompleted(shardID)
		if ok := registry.MarkProcessing(shardID); ok { // fails; already completed
			t.Fatalf("got true; want false")
		}
	})

	t.Run("FindState", func(t *testing.T) {
		shardID := "abc"
		registry := New(time.Hour)
		defer registry.Close()

		if _, ok := registry.FindState(shardID); ok {
			t.Fatalf("got true; want false")
		}

		registry.MarkProcessing(shardID)
		state, ok := registry.FindState(shardID)
		if !ok {
			t.Fatalf("got false; want true")
		}
		if got, want := state, Processing; got != want {
			t.Fatalf("got %v; want %v", got, want)
		}

		registry.MarkCompleted(shardID)
		state, ok = registry.FindState(shardID)
		if !ok {
			t.Fatalf("got false; want true")
		}
		if got, want := state, Completed; got != want {
			t.Fatalf("got %v; want %v", got, want)
		}
	})

	t.Run("Flush", func(t *testing.T) {
		shardID := "abc"
		registry := New(time.Hour)
		defer registry.Close()

		registry.MarkProcessing(shardID)

		n := registry.Flush(time.Now().Add(defaultFlushInterval + time.Hour))
		if got, want := n, 1; got != want {
			t.Fatalf("got %v; want %v", got, want)
		}

		if _, ok := registry.FindState(shardID); ok {
			t.Fatalf("got true; want false")
		}
	})
}
