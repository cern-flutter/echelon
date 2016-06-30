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
	"net/url"
	"os"
	"path"
	"strings"
	"sync"
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
func (this *node) Close() {
	this.mutex.Lock()
	defer this.mutex.Unlock()
	for _, child := range this.children {
		child.Close()
	}
	this.children = nil
}

// String returns a printable representation of the (partial) tree
func (this *node) String() string {
	return this.stringRecursive(0)
}

// stringRecursive serializes to a printable string this node, and recurses
func (this *node) stringRecursive(level int) string {
	this.mutex.RLock()
	defer this.mutex.RUnlock()

	tabs := strings.Repeat("\t", level)
	repr := fmt.Sprintf("%s%s\n%s\n", tabs, this.name, tabs)
	children := make([]string, 0, len(this.children))
	for _, child := range this.children {
		children = append(children, child.stringRecursive(level+1))
	}
	return repr + strings.Join(children, "\n")
}

// Push adds a new object to the tree. The InfoProvider is used to resolve weights.
// This method is recursive.
func (this *node) Push(e *Echelon, route []string, item interface{}) error {
	return this.pushRecursive(e, route, item)
}

// pushRecursive implements Push
func (this *node) pushRecursive(e *Echelon, route []string, element interface{}) error {
	if route[0] != this.name {
		panic("Unexpected echelon traversal")
	}

	// End of the route
	if len(route) == 1 {
		this.mutex.Lock()
		defer this.mutex.Unlock()
		if this.queue == nil {
			var err error
			if this.queue, err = NewQueue(this.dir); err != nil {
				return err
			}
		}
		this.queue.Push(element)
		return nil
	}

	this.mutex.RLock()
	child := this.findChild(route[1])
	this.mutex.RUnlock()

	if child == nil {
		this.mutex.Lock()
		child = &node{
			name:     route[1],
			children: make([]*node, 0, 16),
			dir:      appendDir(this.dir, route[1]),
		}
		this.children = append(this.children, child)
		this.mutex.Unlock()
	}

	this.mutex.RLock()
	defer this.mutex.RUnlock()
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
func (this *node) remove(child *node) {
	// lock should be already acquired by caller
	for index, ptr := range this.children {
		if ptr == child {
			if child.queue != nil {
				child.queue.Close()
			}
			os.RemoveAll(child.dir)
			// Swap last with this one, and shrink
			// See https://github.com/golang/go/wiki/SliceTricks (delete without preserving order)
			count := len(this.children)
			this.children[index] = this.children[count-1]
			this.children = this.children[:count-1]
			return
		}
	}
}

// Empty returns true if the node has no children or queued elements.
func (this *node) Empty() (bool, error) {
	this.mutex.RLock()
	defer this.mutex.RUnlock()

	if this.queue != nil {
		return this.queue.Empty()
	}
	return len(this.children) == 0, nil
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
func (this *node) popLeafNode(element interface{}, e *Echelon, route []string) error {
	// mutex must be hold by caller
	slots, err := e.provider.GetAvailableSlots(route)
	if err != nil {
		return err
	}
	if slots <= 0 {
		return ErrNotEnoughSlots
	}

	err = this.queue.Pop(element)
	if err != nil {
		return err
	}
	return e.provider.ConsumeSlot(route)
}

// popRecursive pops a queued element if there are enough slots available for all the intermediate
// steps in the tree.
func (this *node) popRecursive(element interface{}, e *Echelon, parent []string) error {
	route := append(parent, this.name)

	this.mutex.RLock()

	// Leaf node
	if this.queue != nil {
		this.mutex.RUnlock()
		this.mutex.Lock()
		defer this.mutex.Unlock()
		return this.popLeafNode(element, e, route)
	}

	this.mutex.RUnlock()

	// Available slots for the path so far
	slots, err := e.provider.GetAvailableSlots(route)
	if err != nil {
		return err
	} else if slots <= 0 {
		// Nothing available, so do not even bother recursing
		return ErrNotEnoughSlots
	}

	this.mutex.RLock()

	// Intermediate node
	// We choose a random child based on their weight, and ask recursively
	// Since we may be unlucky enough to pick a path without available slots deeper down,
	// we iterate until we exhaust all possible children
	var selected *node

	possibleChoices := make([]*node, len(this.children))
	copy(possibleChoices, this.children)
	weights := make([]float32, len(this.children))
	childRoute := make([]string, len(route)+1)

	for index, child := range possibleChoices {
		childRoute[len(route)] = child.name
		weights[index] = e.provider.GetWeight(childRoute)
	}

	for len(possibleChoices) > 0 {
		index, child := pickChild(&possibleChoices, &weights)
		if child == nil {
			this.mutex.RUnlock()
			return ErrNilChild
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

	this.mutex.RUnlock()

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
		this.mutex.Lock()
		this.remove(selected)
		this.mutex.Unlock()
	}
	return nil
}

// Pop gets a queued element following the tree using the relative weights, if there are enough slots
// available for the path. If there are no available slots, or nothing queued, then it returns nil.
func (this *node) Pop(element interface{}, e *Echelon) error {
	return this.popRecursive(element, e, []string{})
}
