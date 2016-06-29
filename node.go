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
	"fmt"
	"math/rand"
	"strings"
	"sync"
)

type (
	// Node models nodes inside the queue tree
	node struct {
		mutex sync.RWMutex
		// label, or name, of the node. For instance, "express".
		label string
		// weight of the node.
		weight float32
		// children nodes, as they were inserted
		children []*node
		// queue is a FIFO struct on leaf nodes.
		queue Queue
	}
)

// stringRecursive serializes to a printable string this node, and recurses
func (n *node) stringRecursive(level int) string {
	n.mutex.RLock()
	defer n.mutex.RUnlock()

	tabs := strings.Repeat("\t", level)
	this := fmt.Sprintf("%s%s (%f)\n%s%d queued\n", tabs, n.label, n.weight, tabs, n.queue.Len())
	children := make([]string, 0, len(n.children))
	for _, child := range n.children {
		children = append(children, child.stringRecursive(level+1))
	}
	return this + strings.Join(children, "\n")
}

// String returns a printable representation of the (partial) tree
func (n *node) String() string {
	return n.stringRecursive(0)
}

// FindChild returns the child with the given label.
// nil is returned if not found.
func (n *node) findChild(label string) *node {
	// lock should be already acquired by caller
	for _, child := range n.children {
		if child.label == label {
			return child
		}
	}
	return nil
}

// Push adds a new object to the tree. The InfoProvider is used to resolve weights.
// This method is recursive.
func (n *node) Push(provider InfoProvider, keys []string, labels map[string]string, i interface{}) {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	if len(keys) == 0 {
		n.queue.Push(i)
	} else {
		first, remain := keys[0], keys[1:]
		label := labels[first]
		child := n.findChild(label)
		if child == nil {
			child = &node{
				label:    label,
				weight:   provider.GetWeight(first, label),
				children: make([]*node, 0, 16),
			}
			n.children = append(n.children, child)
		}
		child.Push(provider, remain, labels, i)
	}
}

// Remove removes the child and associated data.
func (n *node) remove(child *node) {
	// lock should be already acquired by caller
	for index, ptr := range n.children {
		if ptr == child {
			// Swap last with this one, and shrink
			// See https://github.com/golang/go/wiki/SliceTricks (delete without preserving order)
			count := len(n.children)
			n.children[index] = n.children[count-1]
			n.children = n.children[:count-1]
			return
		}
	}
}

// Empty returns true if the node has no children or queued elements.
func (n *node) Empty() bool {
	n.mutex.RLock()
	defer n.mutex.RUnlock()
	return n.queue.Len() == 0 && len(n.children) == 0
}

// recalculateRanges updates the relative weights of the childrens.
func recalculateRanges(children *[]*node) map[string]float32 {
	ranges := make(map[string]float32)
	// lock should be already acquired by caller
	total := float32(0.0)
	for _, child := range *children {
		total += child.weight
	}
	accumulator := float32(0)
	for _, child := range *children {
		width := (child.weight / total)
		ranges[child.label] = accumulator + width
		accumulator += width
	}
	return ranges
}

// pickChild chooses a random node according to their weights.
func pickChild(children *[]*node) *node {
	ranges := recalculateRanges(children)
	chance := rand.Float32()
	for _, child := range *children {
		top := ranges[child.label]
		if top >= chance {
			return child
		}
	}
	return nil
}

// dequeueRecursive pops a queued element if there are enough slots available for all the intermediate
// steps in the tree.
func (n *node) popRecursive(provider InfoProvider, path []string) (interface{}, error) {
	path = append(path, n.label)

	n.mutex.RLock()
	nQueue := n.queue.Len()
	nChildren := len(n.children)
	n.mutex.RUnlock()

	// Nothing to do
	if nQueue == 0 && nChildren == 0 {
		return nil, nil
	}

	// Available slots for the path so far
	slots, err := provider.GetAvailableSlots(path)
	if err != nil {
		return nil, err
	}
	// Nothing available, so do not even bother recursing
	if slots <= 0 {
		return nil, nil
	}

	// Leaf node
	if nChildren == 0 {
		n.mutex.Lock()
		defer n.mutex.Unlock()

		element := n.queue.Pop()
		provider.ConsumeSlot(path)
		return element, nil
	}

	// Intermediate node
	// We choose a random child based on their weight, and ask recursively
	n.mutex.RLock()

	child := pickChild(&n.children)
	if child == nil {
		panic("Unexpected nil child")
	}

	element, err := child.popRecursive(provider, path)
	n.mutex.RUnlock()
	if element != nil {
		provider.ConsumeSlot(path)
		// Drop child if empty
		if child.Empty() {
			n.mutex.Lock()
			n.remove(child)
			n.mutex.Unlock()
		}
	}

	return element, err
}

// Pop gets a queued element following the tree using the relative weights, if there are enough slots
// available for the path. If there are no available slots, or nothing queued, then it returns nil.
func (n *node) Pop(provider InfoProvider) (interface{}, error) {
	return n.popRecursive(provider, []string{})
}
