package ddbstreams

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	"github.com/tj/assert"
)

func TestStreams(t *testing.T) {
	if os.Getenv("AWS_ACCESS_KEY_ID") == "" || os.Getenv("AWS_SECRET_ACCESS_KEY") == "" {
		t.SkipNow()
	}

	s, err := session.NewSession(&aws.Config{Region: aws.String("us-west-2")})
	assert.Nil(t, err)

	tableName := "stream"
	api := dynamodbstreams.New(s)

	p := New(api)
	fn := func(ctx context.Context, record *dynamodbstreams.StreamRecord) error {
		id := *record.Keys["id"].S
		log.Println(id, *record.SequenceNumber)
		return nil
	}
	sub, err := p.Subscribe(context.Background(), tableName, fn,
		//WithTrace(log.Println),
		//WithInitialOffsets(Offset{ShardID: "shardId-00000001535311324021-5ff99e5b", SequenceNumber: "37808200000000012727376821"}),
		WithInitialOffsets(Offset{ShardID: "shardId-00000001535311324021-5ff99e5b", SequenceNumber: "37808700000000012727378222"}),
	)
	assert.Nil(t, err)
	defer sub.Close()

	ddb := dynamodb.New(s)
	for i := 1; i <= 1e3; i++ {
		_, err := ddb.PutItem(&dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]*dynamodb.AttributeValue{
				"id":   {S: aws.String(strconv.Itoa(i))},
				"date": {S: aws.String(time.Now().Format(time.RFC850))},
			},
		})
		assert.Nil(t, err)
		time.Sleep(200 * time.Millisecond)
		fmt.Print(".")
	}

	time.Sleep(3 * time.Second)
}
