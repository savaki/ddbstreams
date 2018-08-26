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
	"crypto/rand"
	"encoding/hex"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	"github.com/tj/assert"
)

const hashKey = "id"

func withTable(t *testing.T, callback func(ddb *dynamodb.DynamoDB, streams *dynamodbstreams.DynamoDBStreams, tableName string)) {
	sess := session.Must(session.NewSession(aws.NewConfig().
		WithRegion("us-west-2").
		WithEndpoint("http://localhost:8000").
		WithCredentials(credentials.NewStaticCredentials("blah", "blah", "")),
	))

	ddb := dynamodb.New(sess)
	streams := dynamodbstreams.New(sess)

	content := make([]byte, 8)
	rand.Read(content)
	tableName := "tmp-" + hex.EncodeToString(content)
	input := dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String(hashKey),
				AttributeType: aws.String(dynamodb.ScalarAttributeTypeS),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String(hashKey),
				KeyType:       aws.String(dynamodb.KeyTypeHash),
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(3),
			WriteCapacityUnits: aws.Int64(3),
		},
		StreamSpecification: &dynamodb.StreamSpecification{
			StreamEnabled:  aws.Bool(true),
			StreamViewType: aws.String(dynamodb.StreamViewTypeNewAndOldImages),
		},
	}

	debug.Println("creating table,", tableName)
	_, err := ddb.CreateTable(&input)
	assert.Nil(t, err)

	err = ddb.WaitUntilTableExists(&dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	})
	assert.Nil(t, err)
	debug.Println("created table,", tableName)

	defer func() {
		debug.Println("deleting table,", tableName)
		_, err = ddb.DeleteTable(&dynamodb.DeleteTableInput{TableName: aws.String(tableName)})
		assert.Nil(t, err)

		err = ddb.WaitUntilTableNotExists(&dynamodb.DescribeTableInput{TableName: aws.String(tableName)})
		assert.Nil(t, err)
		debug.Println("table deleted")
	}()

	callback(ddb, streams, tableName)
}

func TestStreams(t *testing.T) {
	t.Parallel()

	withTable(t, func(ddb *dynamodb.DynamoDB, streams *dynamodbstreams.DynamoDBStreams, tableName string) {
		p := New(streams)
		received := map[string]struct{}{}

		fn := func(ctx context.Context, record *dynamodbstreams.StreamRecord) error {
			id := *record.Keys["id"].S
			received[id] = struct{}{}
			return nil
		}

		sub, err := p.Subscribe(context.Background(), tableName, fn)
		assert.Nil(t, err)
		defer sub.Close()

		want := 100
		for i := 1; i <= want; i++ {
			_, err := ddb.PutItem(&dynamodb.PutItemInput{
				TableName: aws.String(tableName),
				Item: map[string]*dynamodb.AttributeValue{
					"id":   {S: aws.String(strconv.Itoa(i))},
					"date": {S: aws.String(time.Now().Format(time.RFC850))},
				},
			})
			assert.Nil(t, err)
		}

		time.Sleep(time.Second)
		assert.Len(t, received, want)
	})
}
