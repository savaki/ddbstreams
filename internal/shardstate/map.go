// Copyright 2018 Matt Ho
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package shardstate

import (
	"context"
	"sync"
	"time"
)

const (
	defaultFlushInterval = time.Hour      // defaultFlushInterval defines interval for flushing states
	maxAge               = 18 * time.Hour // maxAge defines max age of state
)

// State of processing for a given shard
type State string

const (
	NotFound   State = ""           // NotFound indicates shard has not been processed
	Completed  State = "completed"  // Completed indicates processing on shard has been completed
	Processing State = "processing" // Processing indicates the shard is currenting being processed
)

type entry struct {
	State     State
	Timestamp time.Time
}

// Registry contains the state of processing for known shards
type Registry struct {
	cancel      context.CancelFunc
	done        chan struct{}
	mutex       sync.Mutex
	shardStates map[string]entry // shardID -> State
}

// Close the Registry, freeing resources consumed by the Registry
func (r *Registry) Close() error {
	r.cancel()
	<-r.done
	return nil
}

// MarkCompleted marks the specified shard as completed
func (r *Registry) MarkCompleted(shardID string) {
	r.mutex.Lock()
	r.shardStates[shardID] = entry{
		State:     Completed,
		Timestamp: time.Now(),
	}
	r.mutex.Unlock()
}

// MarkProcessing marks the shard as processing if the shard has not been previous been marked.
// Return false if processing has already started (or completed) on the shard; true if it's
// safe to begin work.
func (r *Registry) MarkProcessing(shardID string) bool {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if v, _ := r.shardStates[shardID]; v.State == Completed || v.State == Processing {
		return false
	}

	r.shardStates[shardID] = entry{
		State:     Processing,
		Timestamp: time.Now(),
	}
	return true
}

// FindState returns the current state of the requested shard
func (r *Registry) FindState(shardID string) (State, bool) {
	r.mutex.Lock()
	e, ok := r.shardStates[shardID]
	r.mutex.Unlock()

	if !ok {
		return NotFound, false
	}
	return e.State, true
}

// flushExpiredStates deletes all states that have not been accessed in over 18hrs
// Given that dynamodb only holds 24 hours of stream data, this should be fine.
// Returns the number of records flushed.
func (r *Registry) Flush(cutoff time.Time) (n int) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	for k, v := range r.shardStates {
		if v.Timestamp.Before(cutoff) {
			delete(r.shardStates, k)
			n++
		}
	}

	return n
}

func (r *Registry) mainLoop(ctx context.Context, flush time.Duration) {
	defer close(r.done)

	if flush <= 0 {
		flush = defaultFlushInterval
	}

	ticker := time.NewTicker(flush)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			r.Flush(time.Now().Add(-maxAge))
		}
	}
}

// New constructs a new shard state registry that flushes old records at
// the specified interval.
//
// flush = 0 indicates use the default flush interval, 1hr.
func New(flush time.Duration) *Registry {
	ctx, cancel := context.WithCancel(context.Background())
	registry := &Registry{
		cancel:      cancel,
		done:        make(chan struct{}),
		shardStates: map[string]entry{},
	}
	go registry.mainLoop(ctx, flush)

	return registry
}
