[![GoDoc](https://godoc.org/github.com/savaki/ddbstreams?status.svg)](https://godoc.org/github.com/savaki/ddbstreams)
[![Build Status](https://travis-ci.org/savaki/ddbstreams.svg?branch=master)](https://travis-ci.org/savaki/ddbstreams)

ddbstreams
--------------------------------------------------

`ddbstreams` is a `DynamoDB Streams` consumer.

### Usage

When subscribing to a table with `ddbstreams`, a goroutine will be spawned that periodically 
polls the underlying dynamodb stream for the table. `ddbstreams` handles tracking stream splits.
Records from a given stream shard will be processed in order, but there's no ordering guarantee
between peer stream shards.

### Offsets

`ddbstreams` keeps track of offsets and allows the stream processor to continue from where it
left off similar to a Kafka consumer.

### Example

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	"github.com/savaki/ddbstreams"
)

func main() {
	var (
		cfg       = aws.NewConfig().WithRegion("us-east-1")
		sess      = session.Must(session.NewSession(cfg))
		api       = dynamodbstreams.New(sess)
		processor = ddbstreams.New(api)
		ctx       = context.Background()
		tableName = "blah"
	)

	handler := func(ctx context.Context, record *dynamodbstreams.StreamRecord) error {
		fmt.Println("received record")
		return nil
	}

	sub, err := processor.Subscribe(ctx, tableName, handler)
	if err != nil {
		log.Fatalln(err)
	}
	defer sub.Close()

	// allow time for some records to be processed
	time.Sleep(5 * time.Second)
}
```


### Options

* `WithOffsetManager` - defines storage for persistent offsets
* `WithAutoCommit` - requests offsets be published after every commit.  Only used in
conjunction with `WithOffsetManager` or `WithOffsetManagerDynamoDB`
