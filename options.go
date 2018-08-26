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

import "time"

type Offset struct {
	ShardID        string
	SequenceNumber string
}

type Options struct {
	offsets []Offset
	delay   time.Duration
	debug   func(args ...interface{})
	trace   func(args ...interface{})
}

type Option func(*Options)

func WithInitialOffsets(offsets ...Offset) Option {
	return func(o *Options) {
		o.offsets = offsets
	}
}

// WithDelay indicates delay between polling requests on open (e.g. not complete) shards
func WithDelay(delay time.Duration) Option {
	return func(o *Options) {
		o.delay = delay
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
