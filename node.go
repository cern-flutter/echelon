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
	log "github.com/Sirupsen/logrus"
	"math/rand"
	"strings"
)

type (
	// Node models nodes inside the queue tree
	node struct {
		// label, or name, of the node. For instance, "express".
		name string
		// children nodes, as they were inserted
		children []*node
		// queue is a FIFO struct on leaf nodes.
		queue queue
	}
)

// Close recursively frees resources
func (n *node) Close() {
	for _, child := range n.children {
		child.Close()
	}
	n.children = nil
}

// String returns a printable representation of the (partial) tree
func (n *node) String() string {
	return n.stringRecursive(0)
}

// stringRecursive serializes to a printable string this node, and recurses
func (n *node) stringRecursive(level int) string {
	tabs := strings.Repeat("\t", level)
	repr := fmt.Sprintf("%s%s\n%s\n", tabs, n.name, tabs)
	children := make([]string, 0, len(n.children))
	for _, child := range n.children {
		children = append(children, child.stringRecursive(level+1))
	}
	return repr + strings.Join(children, "\n")
}

// Push adds a new object to the tree. The InfoProvider is used to resolve weights.
// This method is recursive.
func (n *node) push(e *Echelon, route []string, item *queueItem) error {
	route = append([]string{"/"}, route...)
	return n.pushRecursive(e, route, item)
}

// pushRecursive implements Push
func (n *node) pushRecursive(e *Echelon, route []string, item *queueItem) error {
	if route[0] != n.name {
		log.Panicf("Unexpected echelon traversal: %s != %s", route[0], n.name)
	}

	// End of the route
	if len(route) == 1 {
		if n.queue == nil {
			n.queue = newQueue()
			heap.Init(&n.queue)
		}
		heap.Push(&n.queue, item)
		return nil
	}

	child := n.findChild(route[1])

	if child == nil {
		child = &node{
			name:     route[1],
			children: make([]*node, 0, 16),
		}
		n.children = append(n.children, child)
	}

	return child.pushRecursive(e, route[1:], item)
}

// FindChild returns the child with the given label.
// nil is returned if not found.
func (n *node) findChild(label string) *node {
	// lock should be already acquired by caller
	for _, child := range n.children {
		if child.name == label {
			return child
		}
	}
	return nil
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
func (n *node) empty() bool {
	// lock should be already acquired by caller
	if n.queue != nil {
		return n.queue.Len() == 0
	}
	return len(n.children) == 0
}

// recalculateRanges updates the relative weights of the childrens.
func recalculateRanges(weights *[]float32) []float32 {
	// lock should be already acquired by caller
	ranges := make([]float32, len(*weights))
	total := float32(0.0)
	for _, w := range *weights {
		total += w
	}
	accumulator := float32(0)
	for index, w := range *weights {
		width := (w / total)
		ranges[index] = accumulator + width
		accumulator += width
	}
	return ranges
}

// pickChild chooses a random node according to their weights.
func pickChild(children *[]*node, weights *[]float32) (int, *node) {
	// lock should be already acquired by caller
	ranges := recalculateRanges(weights)
	chance := rand.Float32()
	for index, child := range *children {
		top := ranges[index]
		if top >= chance {
			return index, child
		}
	}
	return -1, nil
}

// popFromLeafNode returns the next id from the queue
func (n *node) popLeafNode(e *Echelon, route []string) (*queueItem, error) {
	// lock should be already acquired by caller
	available, err := e.provider.IsThereAvailableSlots(route)
	if err != nil {
		return nil, err
	}
	if !available {
		return nil, ErrNotEnoughSlots
	}

	return heap.Pop(&n.queue).(*queueItem), nil
}

// popRecursive returns the following id
func (n *node) popRecursive(e *Echelon, parent []string) (*queueItem, error) {
	route := append(parent, n.name)

	// Leaf node
	if n.queue != nil {
		return n.popLeafNode(e, route)
	}

	// Available slots for the path so far
	available, err := e.provider.IsThereAvailableSlots(route)
	if err != nil {
		return nil, err
	} else if !available {
		// Nothing available, so do not even bother recursing
		return nil, ErrNotEnoughSlots
	}

	// Intermediate node
	// We choose a random child based on their weight, and ask recursively
	// Since we may be unlucky enough to pick a path without available slots deeper down,
	// we iterate until we exhaust all possible children
	var selected *node
	var item *queueItem

	possibleChoices := make([]*node, len(n.children))
	copy(possibleChoices, n.children)
	weights := make([]float32, len(n.children))
	childRoute := make([]string, len(route)+1)

	for index, child := range possibleChoices {
		childRoute[len(route)] = child.name
		weights[index] = e.provider.GetWeight(childRoute)
	}

	for len(possibleChoices) > 0 {
		index, child := pickChild(&possibleChoices, &weights)
		if child == nil {
			panic("Unexpected nil child")
		}

		item, err = child.popRecursive(e, route)

		if err == ErrNotEnoughSlots {
			// Drop this one and pick another one again, until we run out of children
			possibleChoices[index] = possibleChoices[len(possibleChoices)-1]
			possibleChoices = possibleChoices[:len(possibleChoices)-1]
			weights[index] = weights[len(weights)-1]
			weights = weights[:len(weights)-1]
		} else if err == nil {
			// All good, so this is the chosen one
			selected = child
			break
		} else {
			// Some error we can not handle
			break
		}
	}

	// If the error is set, bail out
	if err != nil {
		return nil, err
	} else if selected == nil {
		// If we haven't got any error, but didn't select anyone, we are empty
		return nil, ErrEmpty
	}

	// Drop child if now empty
	if selected.empty() {
		n.remove(selected)
	}
	return item, nil
}

// Pop gets the id of the next element following the tree using the relative weights, if there are enough slots
// available for the path. If there are no available slots, or nothing queued, then it returns nil.
func (n *node) pop(e *Echelon) (*queueItem, error) {
	return n.popRecursive(e, []string{})
}
