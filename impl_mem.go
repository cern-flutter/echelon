/*
 * Copyright (c) CERN 2016
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package echelon

import (
	"container/heap"
	"fmt"
	"strings"
)

type (
	queue []*QueueEntry

	// Node models nodes inside the queue tree
	MemNode struct {
		// label, or name, of the node. For instance, "express".
		name string
		// children nodes, as they were inserted
		children []*MemNode
		// queue is a FIFO struct on leaf nodes.
		queue queue
	}

	MemNodeStorage struct {
	}
)

// Len returns the length of the queue
func (q queue) Len() int {
	return len(q)
}

// Less returns true if q[i] < q[j]
func (q queue) Less(i, j int) bool {
	return q[i].Priority < q[j].Priority || q[i].Timestamp.Sub(q[j].Timestamp) < 0
}

// Swap swaps the elements i and j
func (q queue) Swap(i, j int) {
	q[i], q[j] = q[j], q[i]
}

// Push adds a new element to the back of the queue
func (q *queue) Push(x interface{}) {
	item := x.(*QueueEntry)
	*q = append(*q, item)
}

// Pop removes an element from the front of the queue.
// It return ErrEmpty if empty
func (q *queue) Pop() interface{} {
	old := *q
	n := len(old)
	item := old[n-1]
	*q = old[0 : n-1]
	return item
}

func (n *MemNode) Name() string {
	return n.name
}

func (n *MemNode) NewChild(label string) (Node, error) {
	child := &MemNode{
		name:     label,
		children: make([]*MemNode, 0, 16),
	}
	n.children = append(n.children, child)
	return child, nil
}

func (n *MemNode) GetChild(label string) (Node, error) {
	// lock should be already acquired by caller
	for _, child := range n.children {
		if child.name == label {
			return child, nil
		}
	}
	return nil, ErrNotFound
}

func (n *MemNode) RemoveChild(target Node) error {
	for index, child := range n.children {
		if child.name == target.Name() {
			// Swap last with this one, and shrink
			// See https://github.com/golang/go/wiki/SliceTricks (delete without preserving order)
			count := len(n.children)
			n.children[index] = n.children[count-1]
			n.children = n.children[:count-1]
			return nil
		}
	}
	return ErrNotFound
}

func (n *MemNode) ChildNames() ([]string, error) {
	names := make([]string, len(n.children))
	for index, child := range n.children {
		names[index] = child.name
	}
	return names, nil
}

func (n *MemNode) Empty() (bool, error) {
	// lock should be already acquired by caller
	if n.queue != nil {
		return n.queue.Len() == 0, nil
	}
	return len(n.children) == 0, nil
}

func (n *MemNode) HasQueued() (bool, error) {
	return n.queue != nil && n.queue.Len() > 0, nil
}

func (n *MemNode) Push(entry *QueueEntry) error {
	heap.Push(&n.queue, entry)
	return nil
}

func (n *MemNode) Pop() (*QueueEntry, error) {
	return heap.Pop(&n.queue).(*QueueEntry), nil
}

// stringRecursive serializes to a printable string this node, and recurses
func (n *MemNode) stringRecursive(level int) string {
	tabs := strings.Repeat("\t", level)
	repr := fmt.Sprintf("%s%s\n%s\n", tabs, n.name, tabs)
	children := make([]string, 0, len(n.children))
	for _, child := range n.children {
		children = append(children, child.stringRecursive(level+1))
	}
	return repr + strings.Join(children, "\n")
}

// String returns a printable representation of the (partial) tree
func (n *MemNode) String() string {
	return n.stringRecursive(0)
}

func (s *MemNodeStorage) Close() error {
	return nil
}

func (s *MemNodeStorage) Root() Node {
	return &MemNode{
		name:     "/",
		children: make([]*MemNode, 0, 8),
	}
}
