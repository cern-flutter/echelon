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
	"errors"
	"fmt"
	"math/rand"
	"net/url"
	"os"
	"path"
	"strings"
	"sync"
)

var (
	// ErrEmpty is returned when the queue has no entries
	ErrEmpty = errors.New("Empty queue")
	// ErrNotEnoughSlots is returned when there are no availability, but there are queued items
	ErrNotEnoughSlots = errors.New("Not enough slots")
)

type (
	// Node models nodes inside the queue tree
	node struct {
		mutex sync.RWMutex
		// label, or name, of the node. For instance, "express".
		name string
		// children nodes, as they were inserted
		children []*node
		// queue is a FIFO struct on leaf nodes.
		queue *Queue
		// dir is the phisical file path where the underlying queues are stored
		dir string
	}
)

// Close recursively frees resources
func (n *node) Close() {
	n.mutex.Lock()
	defer n.mutex.Unlock()
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
	n.mutex.RLock()
	defer n.mutex.RUnlock()

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
func (n *node) Push(e *Echelon, route []string, item interface{}) error {
	return n.pushRecursive(e, route, item)
}

// pushRecursive implements Push
func (n *node) pushRecursive(e *Echelon, route []string, element interface{}) error {
	if route[0] != n.name {
		panic("Unexpected echelon traversal")
	}

	// End of the route
	if len(route) == 1 {
		n.mutex.Lock()
		defer n.mutex.Unlock()
		if n.queue == nil {
			var err error
			if n.queue, err = NewQueue(n.dir); err != nil {
				return err
			}
		}
		n.queue.Push(element)
		return nil
	}

	n.mutex.RLock()
	child := n.findChild(route[1])
	n.mutex.RUnlock()

	if child == nil {
		n.mutex.Lock()
		child = &node{
			name:     route[1],
			children: make([]*node, 0, 16),
			dir:      appendDir(n.dir, route[1]),
		}
		n.children = append(n.children, child)
		n.mutex.Unlock()
	}

	n.mutex.RLock()
	defer n.mutex.RUnlock()
	return child.pushRecursive(e, route[1:], element)
}

// buildFsDir builds the physical  path on disk
func appendDir(base, name string) string {
	return path.Join(base, url.QueryEscape(name))
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
			if child.queue != nil {
				child.queue.Close()
			}
			os.RemoveAll(child.dir)
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
func (n *node) Empty() (bool, error) {
	n.mutex.RLock()
	defer n.mutex.RUnlock()

	if n.queue != nil {
		return n.queue.Empty()
	}
	return len(n.children) == 0, nil
}

// recalculateRanges updates the relative weights of the childrens.
func recalculateRanges(weights *[]float32) []float32 {
	ranges := make([]float32, len(*weights))
	// lock should be already acquired by caller
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

// popFromLeafNode extracts an element from the queue
func (n *node) popLeafNode(element interface{}, e *Echelon, route []string) error {
	// mutex must be hold by caller
	slots, err := e.provider.GetAvailableSlots(route)
	if err != nil {
		return err
	}
	if slots <= 0 {
		return ErrNotEnoughSlots
	}

	err = n.queue.Pop(element)
	if err != nil {
		return err
	}
	return e.provider.ConsumeSlot(route)
}

// popRecursive pops a queued element if there are enough slots available for all the intermediate
// steps in the tree.
func (n *node) popRecursive(element interface{}, e *Echelon, parent []string) error {
	route := append(parent, n.name)

	n.mutex.RLock()

	// Leaf node
	if n.queue != nil {
		n.mutex.RUnlock()
		n.mutex.Lock()
		defer n.mutex.Unlock()
		return n.popLeafNode(element, e, route)
	}

	n.mutex.RUnlock()

	// Available slots for the path so far
	slots, err := e.provider.GetAvailableSlots(route)
	if err != nil {
		return err
	} else if slots <= 0 {
		// Nothing available, so do not even bother recursing
		return ErrNotEnoughSlots
	}

	n.mutex.RLock()

	// Intermediate node
	// We choose a random child based on their weight, and ask recursively
	// Since we may be unlucky enough to pick a path without available slots deeper down,
	// we iterate until we exhaust all possible children
	var selected *node

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
			n.mutex.RUnlock()
			panic("Unexpected nil child")
		}

		err = child.popRecursive(element, e, route)

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

	n.mutex.RUnlock()

	// If the error is set, bail out
	if err != nil {
		return err
	} else if selected == nil {
		// If we haven't got any error, but didn't select anyone, we are empty
		return ErrEmpty
	}

	// Tell the scoreboard we are using a slot
	if err = e.provider.ConsumeSlot(route); err != nil {
		return err
	}

	// Drop child if now empty
	if empty, err := selected.Empty(); err != nil {
		return err
	} else if empty {
		n.mutex.Lock()
		n.remove(selected)
		n.mutex.Unlock()
	}
	return nil
}

// Pop gets a queued element following the tree using the relative weights, if there are enough slots
// available for the path. If there are no available slots, or nothing queued, then it returns nil.
func (n *node) Pop(element interface{}, e *Echelon) error {
	return n.popRecursive(element, e, []string{})
}
