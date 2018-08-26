package ddbstreams

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

type OffsetSaver interface {
	Save(ctx context.Context, groupID, tableName string, offsets ...Offset) error
}

type OffsetFinder interface {
	Find(ctx context.Context, groupID, tableName string) ([]Offset, error)
}

type OffsetManager interface {
	OffsetSaver
	OffsetFinder
}

const (
	ddbHashKey    = "groupID"
	ddbRangeKey   = "tableName"
	ddbOffsetsKey = "offsets"
)

type ddbOffsetManager struct {
	api       dynamodbiface.DynamoDBAPI
	tableName string
}

func (d ddbOffsetManager) createTable(ctx context.Context, logFunc func(...interface{})) error {
	logFunc("creating table,", d.tableName)

	input := dynamodb.CreateTableInput{
		TableName: aws.String(d.tableName),
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String(ddbHashKey),
				AttributeType: aws.String(dynamodb.ScalarAttributeTypeS),
			},
			{
				AttributeName: aws.String(ddbRangeKey),
				AttributeType: aws.String(dynamodb.ScalarAttributeTypeS),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String(ddbHashKey),
				KeyType:       aws.String(dynamodb.KeyTypeHash),
			},
			{
				AttributeName: aws.String(ddbRangeKey),
				KeyType:       aws.String(dynamodb.KeyTypeRange),
			},
		},
		StreamSpecification: &dynamodb.StreamSpecification{
			StreamEnabled:  aws.Bool(true),
			StreamViewType: aws.String(dynamodb.StreamViewTypeNewAndOldImages),
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(3),
			WriteCapacityUnits: aws.Int64(3),
		},
	}

	if _, err := d.api.CreateTableWithContext(ctx, &input); err != nil {
		return err
	}

	return nil
}

func (d ddbOffsetManager) ensureTableExists(ctx context.Context, logFunc func(...interface{})) error {
	input := dynamodb.DescribeTableInput{
		TableName: aws.String(d.tableName),
	}

	_, err := d.api.DescribeTableWithContext(ctx, &input)
	if err == nil {
		return nil
	}

	if v, ok := err.(awserr.Error); !ok || v.Code() != dynamodb.ErrCodeResourceNotFoundException {
		return err // some error besides table does not exist
	}

	logFunc("table not found,", d.tableName)
	if err := d.createTable(ctx, logFunc); err != nil {
		return err
	}

	logFunc("wait for table to exist,", d.tableName)
	if err := d.api.WaitUntilTableExists(&input); err != nil {
		return err
	}

	logFunc("successfully created table,", d.tableName)
	return nil
}

func (d ddbOffsetManager) Save(ctx context.Context, groupID, tableName string, offsets ...Offset) error {
	item, err := dynamodbattribute.Marshal(offsets)
	if err != nil {
		return err
	}

	input := dynamodb.UpdateItemInput{
		TableName: aws.String(d.tableName),
		Key: map[string]*dynamodb.AttributeValue{
			ddbHashKey:  {S: aws.String(groupID)},
			ddbRangeKey: {S: aws.String(tableName)},
		},
		UpdateExpression: aws.String("SET #offsets = :offsets"),
		ExpressionAttributeNames: map[string]*string{
			"#offsets": aws.String(ddbOffsetsKey),
		},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":offsets": item,
		},
	}

	if _, err := d.api.UpdateItemWithContext(ctx, &input); err != nil {
		return err
	}

	return nil
}

func (d ddbOffsetManager) Find(ctx context.Context, groupID, tableName string) ([]Offset, error) {
	input := dynamodb.GetItemInput{
		TableName: aws.String(d.tableName),
		Key: map[string]*dynamodb.AttributeValue{
			ddbHashKey:  {S: aws.String(groupID)},
			ddbRangeKey: {S: aws.String(tableName)},
		},
		ConsistentRead: aws.Bool(true),
	}

	out, err := d.api.GetItemWithContext(ctx, &input)
	if err != nil {
		return nil, err
	}
	if len(out.Item) == 0 || out.Item[ddbOffsetsKey] == nil {
		return nil, nil
	}

	var offsets []Offset
	if err := dynamodbattribute.Unmarshal(out.Item[ddbOffsetsKey], &offsets); err != nil {
		return nil, err
	}

	return offsets, nil
}
