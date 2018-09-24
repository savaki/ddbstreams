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
	"encoding/hex"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
)

const hashKey = "id"

var r = rand.New(rand.NewSource(time.Now().UnixNano()))

func alphaN(n int) string {
	const letters = `ABCDEFGHIJKLMNOPQRSTUVWXYZ`
	data := make([]byte, 0, n)
	for i := 0; i < n; i++ {
		data = append(data, letters[r.Intn(len(letters))])
	}
	return string(data)
}

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
	if err != nil {
		t.Fatalf("got %v; want nil", err)
	}

	err = ddb.WaitUntilTableExists(&dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	})
	if err != nil {
		t.Fatalf("got %v; want nil", err)
	}
	debug.Println("created table,", tableName)

	defer func() {
		debug.Println("deleting table,", tableName)
		_, err = ddb.DeleteTable(&dynamodb.DeleteTableInput{TableName: aws.String(tableName)})
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		err = ddb.WaitUntilTableNotExists(&dynamodb.DescribeTableInput{TableName: aws.String(tableName)})
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
		debug.Println("table deleted")
	}()

	callback(ddb, streams, tableName)
}

func createRecords(t *testing.T, ddb *dynamodb.DynamoDB, tableName string, n int) {
	for i := 1; i <= n; i++ {
		_, err := ddb.PutItem(&dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]*dynamodb.AttributeValue{
				"id":   {S: aws.String(strconv.Itoa(i))},
				"date": {S: aws.String(time.Now().Format(time.RFC850))},
			},
		})
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
	}
}

func TestStreamContinuesFromLastCommit(t *testing.T) {
	t.Parallel()

	withTable(t, func(ddb *dynamodb.DynamoDB, streams *dynamodbstreams.DynamoDBStreams, tableName string) {
		n := 20
		createRecords(t, ddb, tableName, n)

		var sub1 *Subscriber
		var sub2 *Subscriber
		var err error

		received := 0
		part1 := func(ctx context.Context, record *dynamodbstreams.StreamRecord) error {
			received++
			if received >= n/2 && sub1 != nil {
				sub1.Flush()
				sub1.Close()
				return nil
			}

			fmt.Println("1 -", *record.NewImage["id"].S)
			return nil
		}
		part2 := func(ctx context.Context, record *dynamodbstreams.StreamRecord) error {
			fmt.Println("2 -", *record.NewImage["id"].S)

			received++
			if received >= n {
				sub2.Close()
			}
			return nil
		}

		groupID := "abc"
		commitTable := "commits-" + alphaN(10)
		p := New(streams)

		sub1, err = p.Subscribe(context.Background(), tableName, part1,
			WithGroupID(groupID),
			WithOffsetManagerDynamoDB(ddb, commitTable),
		)
		sub1.Wait()

		sub2, err = p.Subscribe(context.Background(), tableName, part2,
			WithGroupID(groupID),
			WithOffsetManagerDynamoDB(ddb, commitTable),
		)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
		sub2.Wait()

		time.Sleep(500 * time.Millisecond)
	})
}

func TestStreams(t *testing.T) {
	t.Parallel()

	withTable(t, func(ddb *dynamodb.DynamoDB, streams *dynamodbstreams.DynamoDBStreams, tableName string) {
		n := 20
		createRecords(t, ddb, tableName, n)

		var sub *Subscriber
		var err error

		received := 0
		handler := func(ctx context.Context, record *dynamodbstreams.StreamRecord) error {
			fmt.Println(*record.NewImage["id"].S)

			received++
			if received == n {
				sub.Flush()
				sub.Close()
				return nil
			}

			return nil
		}

		p := New(streams)
		sub, err = p.Subscribe(context.Background(), tableName, handler)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
		sub.Wait()
	})
}

func TestStreamsAutoCommit(t *testing.T) {
	t.Parallel()

	withTable(t, func(ddb *dynamodb.DynamoDB, streams *dynamodbstreams.DynamoDBStreams, tableName string) {
		n := 20
		createRecords(t, ddb, tableName, n)

		var sub *Subscriber
		var err error

		received := 0
		handler := func(ctx context.Context, record *dynamodbstreams.StreamRecord) error {
			fmt.Println(*record.NewImage["id"].S)

			received++
			if received == n {
				sub.Flush()
				sub.Close()
				return nil
			}

			return nil
		}

		p := New(streams)
		sub, err = p.Subscribe(context.Background(), tableName, handler,
			WithTrace(log.Println),
			WithGroupID("abc"),
			WithOffsetManagerDynamoDB(ddb, "offsets"),
			WithAutoCommit(),
		)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
		sub.Wait()
	})
}
