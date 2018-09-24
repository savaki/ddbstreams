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
	err := mgr.createTableIfNotExists(ctx, debug.Println)
	if err != nil {
		t.Fatalf("got %v; want nil", err)
	}

	defer func() {
		debug.Println("deleting table,", mgr.tableName)
		_, err = api.DeleteTable(&dynamodb.DeleteTableInput{TableName: aws.String(mgr.tableName)})
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		err = api.WaitUntilTableNotExists(&dynamodb.DescribeTableInput{TableName: aws.String(mgr.tableName)})
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
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
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}

		offsets, err := mgr.Find(ctx, groupID, tableName)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
		if got, want := len(offsets), 1; got != want {
			t.Fatalf("got %v; want %v", got, want)
		}
	})
}
