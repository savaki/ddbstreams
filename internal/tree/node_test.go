package tree

import (
	"encoding/json"
	"io/ioutil"
	"testing"

	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	"github.com/tj/assert"
)

func TestParse(t *testing.T) {
	data, err := ioutil.ReadFile("testdata/ordered.json")
	assert.Nil(t, err)

	var shards []*dynamodbstreams.Shard
	assert.Nil(t, json.Unmarshal(data, &shards))

	tree := Parse(shards)
	assert.NotNil(t, tree)
	assert.Len(t, tree.Children, 1)

	t.Run("reversed", func(t *testing.T) {
		var reversed []*dynamodbstreams.Shard
		for i := len(shards) - 1; i >= 0; i-- {
			reversed = append(reversed, shards[i])
		}

		reverseTree := Parse(reversed)
		assert.EqualValues(t, tree, reverseTree)
	})
}
