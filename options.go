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
	offsets        []Offset
	pollInterval   time.Duration
	debug          func(args ...interface{})
	trace          func(args ...interface{})
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

func WithOffsetInDynamoDB(api dynamodbiface.DynamoDBAPI, tableName string) Option {
	return func(o *Options) {
		o.offsetManager = ddbOffsetManager{
			api:       api,
			tableName: tableName,
		}
	}
}

func WithOffsetInterval(interval time.Duration) Option {
	return func(o *Options) {
		o.offsetInterval = interval
	}
}
