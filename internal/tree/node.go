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
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
)

// Node contains a Shard and all its descendants
type Node struct {
	Shard    *dynamodbstreams.Shard `json:"shard"`
	Parent   *Node                  `json:"-"`
	Children []*Node                `json:"children"`
}

func (n *Node) appendNode(node *Node) {
	node.Parent = n
	n.Children = append(n.Children, node)
}

func (n *Node) appendShard(shard *dynamodbstreams.Shard) {
	node := Node{
		Shard:  shard,
		Parent: n,
	}
	n.Children = append(n.Children, &node)
}

// Find the shard with the specified id
func (n *Node) Find(shardID *string) (*Node, bool) {
	if n == nil {
		return nil, false
	}

	if n.Shard != nil && n.Shard.ShardId != nil && *n.Shard.ShardId == *shardID {
		return n, true
	}

	for _, child := range n.Children {
		if item, ok := child.Find(shardID); ok {
			return item, true
		}
	}

	return nil, false
}

// DFS depth first search; true indicates to descend to children
func (n *Node) DFS(fn func(shard *dynamodbstreams.Shard) bool) {
	if n == nil {
		return
	}

	if n.Shard != nil {
		if ok := fn(n.Shard); !ok {
			return
		}
	}

	for _, child := range n.Children {
		child.DFS(fn)
	}
}

// Parse the complete shard list and reassemble it into a tree structure
func Parse(shards []*dynamodbstreams.Shard) *Node {
	root := &Node{}

	for _, shard := range shards {
		if node, ok := root.Find(shard.ParentShardId); ok {
			node.appendShard(shard)
			continue
		}

		root.appendShard(shard)
	}

	var children []*Node
	for _, child := range root.Children {
		if child.Shard.ParentShardId == nil {
			children = append(children, child)
			continue
		}

		if node, ok := root.Find(child.Shard.ParentShardId); ok {
			node.appendNode(child)
			continue
		}

		children = append(children, child)
	}
	root.Children = children

	return root
}
