package ddbstreams

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams/dynamodbstreamsiface"
	"github.com/pkg/errors"
	"github.com/savaki/ddbstreams/internal/tree"
)

func lookupStreamArn(ctx context.Context, api dynamodbstreamsiface.DynamoDBStreamsAPI, tableName string) (string, error) {
	out, err := api.ListStreamsWithContext(ctx, &dynamodbstreams.ListStreamsInput{
		TableName: aws.String(tableName),
	})
	if err != nil {
		return "", errors.Wrapf(err, "unable to list streams for table, %v", tableName)
	}
	if len(out.Streams) == 0 {
		return "", errors.Wrapf(err, "no stream returned for table, %v", tableName)
	}

	streamArn := out.Streams[0].StreamArn
	if streamArn == nil {
		return "", errors.Wrapf(err, "no stream arn found for table, %v", tableName)
	}

	return *streamArn, nil
}

func describeShards(ctx context.Context, api dynamodbstreamsiface.DynamoDBStreamsAPI, streamArn, tableName string) ([]*dynamodbstreams.Shard, error) {
	var lastShardId *string
	var shards []*dynamodbstreams.Shard

	for {
		input := dynamodbstreams.DescribeStreamInput{
			Limit:                 aws.Int64(100),
			StreamArn:             &streamArn,
			ExclusiveStartShardId: lastShardId,
		}
		out, err := api.DescribeStreamWithContext(ctx, &input)
		if err != nil {
			return nil, errors.Wrapf(err, "unable to describe streams for dynamodb table, %v", tableName)
		}

		shards = append(shards, out.StreamDescription.Shards...)
		lastShardId = out.StreamDescription.LastEvaluatedShardId
		if lastShardId == nil {
			return shards, nil
		}
	}
}

func fetchShardTree(ctx context.Context, api dynamodbstreamsiface.DynamoDBStreamsAPI, streamArn string, tableName string) (*tree.Node, error) {
	shards, err := describeShards(ctx, api, streamArn, tableName)
	if err != nil {
		return nil, err
	}

	return tree.Parse(shards), nil
}
