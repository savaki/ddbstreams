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
	"time"

	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

type Offset struct {
	ShardID        string
	SequenceNumber string
}

type Options struct {
	autoCommit     bool          // autoCommit forces publish after every commit
	offsets        []Offset      // offsets holds optional starting offsets
	pollInterval   time.Duration // pollInterval specifies time between polling an open stream
	debug          func(args ...interface{})
	trace          func(args ...interface{})
	groupID        string
	offsetManager  OffsetManager
	offsetInterval time.Duration
}

type Option func(*Options)

func WithInitialOffsets(offsets ...Offset) Option {
	return func(o *Options) {
		o.offsets = offsets
	}
}

// WithPollInterval indicates delay between polling requests on open (e.g. not complete) shards
func WithPollInterval(pollInterval time.Duration) Option {
	return func(o *Options) {
		o.pollInterval = pollInterval
	}
}

// WithDebug indicates delay between polling requests on open (e.g. not complete) shards
func WithDebug(logFunc func(args ...interface{})) Option {
	return func(o *Options) {
		o.debug = logFunc
	}
}

// WithTrace indicates delay between polling requests on open (e.g. not complete) shards
func WithTrace(printFunc func(args ...interface{})) Option {
	return func(o *Options) {
		o.trace = printFunc
	}
}

func WithGroupID(groupID string) Option {
	return func(o *Options) {
		o.groupID = groupID
	}
}

// WithAutoCommit indicates offsets should be saved after each successful commit.
// Caution should be used when enabling this as streams with high traffic will
// generate a significant number of commits.
func WithAutoCommit() Option {
	return func(o *Options) {
		o.autoCommit = true
	}
}

func WithOffsetManager(manager OffsetManager) Option {
	return func(o *Options) {
		o.offsetManager = manager
	}
}

func WithOffsetManagerDynamoDB(api dynamodbiface.DynamoDBAPI, tableName string) Option {
	return WithOffsetManager(ddbOffsetManager{
		api:       api,
		tableName: tableName,
	})
}

func WithOffsetInterval(interval time.Duration) Option {
	return func(o *Options) {
		o.offsetInterval = interval
	}
}
