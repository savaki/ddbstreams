[![GoDoc](https://godoc.org/github.com/savaki/ddbstreams?status.svg)](https://godoc.org/github.com/savaki/ddbstreams)

ddbstreams
--------------------------------------------------

`ddbstreams` is a `DynamoDB Streams` consumer.

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