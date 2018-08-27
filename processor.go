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

	"github.com/aws/aws-sdk-go/service/dynamodbstreams/dynamodbstreamsiface"
)

type Processor struct {
	api dynamodbstreamsiface.DynamoDBStreamsAPI
}

func (p *Processor) Subscribe(ctx context.Context, tableName string, h HandlerFunc, opts ...Option) (*Subscriber, error) {
	var options Options
	for _, opt := range opts {
		opt(&options)
	}

	streamArn, err := lookupStreamArn(ctx, p.api, tableName)
	if err != nil {
		return nil, err
	}

	return newSubscriber(ctx, subscriberConfig{
		api:            p.api,
		tableName:      tableName,
		streamArn:      streamArn,
		handler:        h,
		offsets:        options.offsets,
		groupID:        options.groupID,
		offsetManager:  options.offsetManager,
		offsetInterval: options.offsetInterval,
		pollInterval:   options.pollInterval,
		debug:          options.debug,
		trace:          options.trace,
	})
}

func New(api dynamodbstreamsiface.DynamoDBStreamsAPI) *Processor {
	return &Processor{api: api}
}
