package ddbstreams

import (
	"context"
	"io/ioutil"
	"log"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/tj/assert"
)

var debug = log.New(ioutil.Discard, log.Prefix(), log.LstdFlags)

func TestOffsetManager(t *testing.T) {
	t.Parallel()

	var (
		cfg = aws.NewConfig().
			WithRegion("us-west-2").
			WithEndpoint("http://localhost:8000").
			WithCredentials(credentials.NewStaticCredentials("blah", "blah", ""))
		sess = session.Must(session.NewSession(cfg))
		api  = dynamodb.New(sess)
		ctx  = context.Background()
	)

	mgr := ddbOffsetManager{
		api:       api,
		tableName: "commits",
	}
	err := mgr.ensureTableExists(ctx, debug.Println)
	assert.Nil(t, err)

	defer func() {
		debug.Println("deleting table,", mgr.tableName)
		_, err = api.DeleteTable(&dynamodb.DeleteTableInput{TableName: aws.String(mgr.tableName)})
		assert.Nil(t, err)

		err = api.WaitUntilTableNotExists(&dynamodb.DescribeTableInput{TableName: aws.String(mgr.tableName)})
		assert.Nil(t, err)
		debug.Println("table deleted")
	}()

	t.Run("lifecycle", func(t *testing.T) {
		groupID := "abc"
		tableName := "blah"
		offset := Offset{
			ShardID:        "def",
			SequenceNumber: "ghi",
		}

		err := mgr.Save(ctx, groupID, tableName, offset)
		assert.Nil(t, err)

		offsets, err := mgr.Find(ctx, groupID, tableName)
		assert.Nil(t, err)
		assert.Len(t, offsets, 1)
	})
}
