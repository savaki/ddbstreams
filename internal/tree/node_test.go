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

package tree

import (
	"encoding/json"
	"io/ioutil"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
)

func TestParse(t *testing.T) {
	data, err := ioutil.ReadFile("testdata/ordered.json")
	if err != nil {
		t.Fatalf("got %v; want nil", err)
	}

	var shards []*dynamodbstreams.Shard
	err = json.Unmarshal(data, &shards)
	if err != nil {
		t.Fatalf("got %v; want nil", err)
	}

	tree := Parse(shards)
	if tree == nil {
		t.Fatalf("got nil; want not nill")
	}
	if got, want := len(tree.Children), 1; got != want {
		t.Fatalf("got %v; want %v", got, want)
	}

	t.Run("reversed", func(t *testing.T) {
		var reversed []*dynamodbstreams.Shard
		for i := len(shards) - 1; i >= 0; i-- {
			reversed = append(reversed, shards[i])
		}

		reverseTree := Parse(reversed)
		if got, want := reverseTree, tree; !reflect.DeepEqual(got, want) {
			t.Fatalf("got %v; want %v", got, want)
		}
	})

	t.Run("DFS", func(t *testing.T) {
		var got []string
		tree.DFS(func(shard *dynamodbstreams.Shard) bool {
			got = append(got, *shard.ShardId)
			return true
		})

		want := []string{
			"shardId-00000001535083271146-6e0ef3a5",
			"shardId-00000001535098920133-05e10aba",
			"shardId-00000001535112349034-12bd2c58",
			"shardId-00000001535126119232-2c87677d",
			"shardId-00000001535139002824-8551ec4b",
			"shardId-00000001535151610690-856f1cc6",
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %v; want %v", got, want)
		}
	})
}
