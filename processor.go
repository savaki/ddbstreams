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

	sub := newSubscriber(ctx, subscriberConfig{
		api:       p.api,
		tableName: tableName,
		streamArn: streamArn,
		handler:   h,
		offsets:   options.offsets,
		delay:     options.delay,
		debug:     options.debug,
		trace:     options.trace,
	})

	return sub, nil
}

func New(api dynamodbstreamsiface.DynamoDBStreamsAPI) *Processor {
	return &Processor{api: api}
}
