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
