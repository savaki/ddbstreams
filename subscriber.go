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

package ddbstreams

import (
	"context"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams/dynamodbstreamsiface"
	"github.com/savaki/ddbstreams/internal/shardstate"
)

const (
	limit                 = int64(1000)
	defaultPollInterval   = time.Second
	defaultOffsetInterval = time.Minute
)

type offset struct {
	SequenceNumber string
	CommittedAt    time.Time
}

// HandlerFunc wraps custom stream processing code
type HandlerFunc func(ctx context.Context, record *dynamodbstreams.StreamRecord) error

// Subscriber reference the subscription to the stream
type Subscriber struct {
	cancel  context.CancelFunc
	done    chan struct{}
	wg      sync.WaitGroup
	config  subscriberConfig
	states  *shardstate.Registry // states holds processing state for shards
	refresh chan struct{}        // refresh shard tree request; new shards may be present

	mutex   sync.Mutex
	offsets map[string]offset
}

// Close the subscription, freeing any consumed resources
func (s *Subscriber) Close() error {
	s.config.debug("cancel()")
	s.cancel()
	s.config.debug("<-s.done")
	<-s.done
	s.config.debug("s.wg.Wait()")
	s.wg.Wait()
	s.config.debug("return nil")
	return nil
}

func (s *Subscriber) commit(shardID, sequenceNumber string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.config.trace(shardID, "commit", sequenceNumber)
	s.offsets[shardID] = offset{
		SequenceNumber: sequenceNumber,
		CommittedAt:    time.Now(),
	}
}

func (s *Subscriber) readRecords(ctx context.Context, shardID string, iterator *string, completed bool) (err error) {
	s.config.debug(shardID, "begin read records")
	defer func(begin time.Time) {
		s.config.debug(shardID, "end read records ->", err, time.Now().Sub(begin).Round(time.Millisecond))
	}(time.Now())

	for {
		s.config.trace(shardID, "get records", *iterator)
		out, err := s.config.api.GetRecordsWithContext(ctx, &dynamodbstreams.GetRecordsInput{
			Limit:         aws.Int64(limit),
			ShardIterator: iterator,
		})
		if err != nil {
			s.config.debug("unable to retrieve shard iterator,", err)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(30 * time.Second): // todo change to dynamic backoff
			}
			continue
		}

		s.config.trace(shardID, "retrieved", len(out.Records), "records")

		for _, record := range out.Records {
			if err := s.config.handler(ctx, record.Dynamodb); err != nil {
				s.config.debug("handler failed with err,", err)
				s.cancel()
				return err
			}

			if record.Dynamodb != nil && record.Dynamodb.SequenceNumber != nil {
				s.commit(shardID, *record.Dynamodb.SequenceNumber)
			}
		}

		if out.NextShardIterator == nil {
			return nil
		}

		iterator = out.NextShardIterator

		if len(out.Records) > 0 {
			continue // fetch more records immediately if we received some
		}
		if completed {
			continue // if the shard was completed, process records without delay
		}

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(s.config.pollInterval):
		}
	}
}

func (s *Subscriber) readShard(ctx context.Context, shard *dynamodbstreams.Shard, lastSequenceNumber string, completed bool) {
	if ok := s.states.MarkProcessing(*shard.ShardId); !ok {
		return
	}

	s.config.trace("reading shard (", *shard.ShardId, ") from sequence number,", lastSequenceNumber)
	defer s.config.trace("closing shard (", *shard.ShardId, "),", lastSequenceNumber)

	var sequenceNumber *string
	if lastSequenceNumber != "" {
		sequenceNumber = &lastSequenceNumber
	}

	var iteratorType string
	if sequenceNumber != nil {
		iteratorType = dynamodbstreams.ShardIteratorTypeAfterSequenceNumber
	} else {
		iteratorType = dynamodbstreams.ShardIteratorTypeTrimHorizon
	}

	out, err := s.config.api.GetShardIteratorWithContext(ctx, &dynamodbstreams.GetShardIteratorInput{
		SequenceNumber:    sequenceNumber,
		ShardId:           shard.ShardId,
		ShardIteratorType: aws.String(iteratorType),
		StreamArn:         aws.String(s.config.streamArn),
	})
	if err != nil {
		s.config.debug("unable to retrieve shard iterator,", err)
		return
	}

	if err := s.readRecords(ctx, *shard.ShardId, out.ShardIterator, completed); err != nil {
		s.config.debug("unable to process shard", err)
		return
	}

	s.states.MarkCompleted(*shard.ShardId)

	select {
	case <-ctx.Done():
		return
	case s.refresh <- struct{}{}:
	}
}

func (s *Subscriber) spawnAll(ctx context.Context, offsets []Offset) error {
	s.config.trace("fetch shard tree")
	root, err := fetchShardTree(ctx, s.config.api, s.config.streamArn, s.config.tableName)
	if err != nil {
		return err
	}

	for _, offset := range offsets {
		node, ok := root.Find(&offset.ShardID)
		if !ok {
			continue
		}

		// mark parents as completed
		//
		for n := node.Parent; n != nil; n = n.Parent {
			if n.Shard != nil && n.Shard.ShardId != nil {
				s.states.MarkCompleted(*n.Shard.ShardId)
			}
		}

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			completed := node.Shard.SequenceNumberRange.EndingSequenceNumber != nil
			s.readShard(ctx, node.Shard, offset.SequenceNumber, completed)
		}()
	}

	root.DFS(func(shard *dynamodbstreams.Shard) bool {
		state, ok := s.states.FindState(*shard.ShardId)
		s.config.trace(*shard.ShardId, "states.FindState", state, ok)

		if !ok {
			// read shard; stop dfs
			s.wg.Add(1)
			go func() {
				defer s.wg.Done()
				completed := shard.SequenceNumberRange.EndingSequenceNumber != nil
				s.readShard(ctx, shard, "", completed)
			}()
			return false
		}

		if state == shardstate.Processing {
			// already in progress; stop dfs
			return false //
		}

		return true // keep going
	})
	//os.Exit(1)

	return nil
}

func (s *Subscriber) publishOffsets(ctx context.Context) {
	if s.config.offsetManager == nil || s.config.groupID == "" {
		return
	}

	var offsets []Offset

	s.mutex.Lock()
	for shardID, v := range s.offsets {
		offsets = append(offsets, Offset{
			ShardID:        shardID,
			SequenceNumber: v.SequenceNumber,
		})
	}
	s.mutex.Unlock()

	for attempt := 1; attempt < 4; attempt++ {
		if err := s.config.offsetManager.Save(ctx, s.config.groupID, s.config.tableName, offsets...); err != nil {
			s.config.debug("unable to save offsets,", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Duration(attempt*attempt) * time.Second):
			}
		}

		break
	}

	s.config.debug("successfully saved offsets")
}

func (s *Subscriber) mainLoop(ctx context.Context, offsets []Offset) {
	defer close(s.done)

	s.config.trace("main loop")
	defer s.config.trace("end main loop")

	// spawn initial shard readers
	//
	if err := s.spawnAll(ctx, offsets); err != nil {
		s.config.debug("failed to spawn readers -", err)
	} else {
		offsets = nil // only need offsets until the first successful spawning
	}

	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	offsetTicker := time.NewTicker(s.config.offsetInterval)
	defer offsetTicker.Stop()

	// periodically rebuild shard tree and launch reader for any new
	// shards we may come across
	//
	for {
		select {
		case <-ctx.Done():
			return

		case <-s.refresh:
			if err := s.spawnAll(ctx, nil); err != nil {
				continue
			}

		case <-offsetTicker.C:
			go s.publishOffsets(ctx)

		case <-ticker.C:
			if err := s.spawnAll(ctx, offsets); err != nil {
				continue
			}
			offsets = nil
		}
	}
}

type subscriberConfig struct {
	api            dynamodbstreamsiface.DynamoDBStreamsAPI
	tableName      string                    // tableName being read from
	streamArn      string                    // streamArn being read from
	handler        HandlerFunc               // handler that will process the records
	offsets        []Offset                  // offsets hold initial starting positions (optional)
	pollInterval   time.Duration             // delay between polling (when no records returned)
	groupID        string                    // groupID uniquely identifier the subscriber; similar to Kafka groupID
	offsetManager  OffsetManager             // offsetManager defines save and restore of offsets
	offsetInterval time.Duration             // offsetInterval defines interval to commit offsets
	debug          func(args ...interface{}) // log provides generic logging interface
	trace          func(args ...interface{}) // debug provides generic logging interface
}

func newSubscriber(ctx context.Context, config subscriberConfig) *Subscriber {
	if config.debug == nil {
		config.debug = config.trace
	}
	if config.debug == nil {
		config.debug = func(...interface{}) {}
	}
	if config.trace == nil {
		config.trace = func(...interface{}) {}
	}
	if config.pollInterval == 0 {
		config.pollInterval = defaultPollInterval
	}
	if config.offsetInterval == 0 {
		config.offsetInterval = defaultOffsetInterval
	}

	config.debug("subscribing to stream arn;", config.streamArn)

	ctx, cancel := context.WithCancel(ctx)
	sub := &Subscriber{
		cancel:  cancel,
		done:    make(chan struct{}),
		config:  config,
		states:  shardstate.New(0),
		refresh: make(chan struct{}),
		offsets: map[string]offset{},
	}
	go sub.mainLoop(ctx, config.offsets)
	return sub
}
