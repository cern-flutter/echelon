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
	"log"
	"math/rand"
	"sync"
	"time"
)

var (
	// ErrEmpty is returned when the queue has no entries
	ErrEmpty = errors.New("Empty queue")
	// ErrNotEnoughSlots is returned when there are no availability, but there are queued items
	ErrNotEnoughSlots = errors.New("Not enough slots")
	// ErrNotFound is returned when the id is not found on the persistence DB
	ErrNotFound = errors.New("Not found on the DB")
)

type (
	// StorageIterator is used by the storage interface to iterate stored values.
	StorageIterator interface {
		// Next gets the next value, returns false when there are no more entries
		Next() bool
		// Key returns the current element key
		Key() string
		// Object returns the current object
		Object(object interface{}) error
		// Close releases the iterator
		Close()
	}

	// Storage is an interface that different persistence backends must implement
	// to be used by Echelon.
	// Echelon only requires an iterable key-value store.
	Storage interface {
		// Close closes the storage interface
		// It will  be called when Echelon is closed too
		Close() error
		// Put stores an object
		Put(key string, object interface{}) error
		// Get gets the object with the given key
		// If the object does not exist, return ErrNotFound
		Get(key string, object interface{}) error
		// Delete removes an object from the storage
		Delete(key string) error
		// NewIterator returns an iterator for the storage
		NewIterator() StorageIterator
	}

	// InfoProvider is an interface used by Echelon to determine how to pick the elements.
	InfoProvider interface {
		// GetWeigth is called to determine the weight of the value within the given field
		// For instance, for field = activity and value = express, weight = 10
		GetWeight(route []string) float32
		// IsThereAvailableSlots must return true if there are slots for the given path
		// (i.e. [/, destination], or [/, destination, vo, activity, source])
		// It is up to the provider do decide on how to calculate these (using all, part,
		// always a big enough number...)
		IsThereAvailableSlots(route []string) (bool, error)
	}

	// Item is an interface that elements to be stored on an Echelon queue must implement.
	Item interface {
		// GetID returns an uniquely identifier for this item
		GetID() string
		// GetPath returns a slice of strings that determine the queue for the element
		// (i.e. [SourceSe, DestSe, Vo, Activity])
		GetPath() []string
		// GetTimestamp returns a timestamp using for ordering the events
		GetTimestamp() time.Time
	}

	QueueEntry struct {
		ID        string
		Timestamp time.Time
		Priority  int
	}

	Node interface {
		String() string
		Name() string

		NewChild(name string) Node
		GetChild(name string) Node
		RemoveChild(name string)
		ChildNames() []string

		Empty() bool
		HasQueued() bool
		Push(*QueueEntry) error
		Pop() (*QueueEntry, error)
	}

	NodeStorage interface {
		Close() error
		Root() Node
	}

	// Echelon contains a queue modeled like a tree where each child has a weight.
	// On the leaves, there will be an actual queue where FIFO is performed.
	Echelon struct {
		provider  InfoProvider
		keys      []string
		root      Node
		mutex     sync.RWMutex
		db        Storage
		nodes     NodeStorage
		prototype Item
	}
)

// New returns a new Echelon instance. The caller must pass an InfoProvider that will keep,
// if necessary, scoreboards, resource control, and/or the like.
func New(prototype Item, db Storage, nodes NodeStorage, provider InfoProvider) (*Echelon, error) {
	return &Echelon{
		prototype: prototype,
		db:        db,
		nodes:     nodes,
		provider:  provider,
		root:      nodes.Root(),
	}, nil
}

// Close frees resources
func (e *Echelon) Close() error {
	e.db.Close()
	return e.nodes.Close()
}

// String is a convenience method to generate a printable representation of the content of an
// Echelon instance.
func (e *Echelon) String() string {
	e.mutex.RLock()
	defer e.mutex.RUnlock()
	return fmt.Sprintf("Keys: %v\nQueue:\n%v", e.keys, e.root)
}

// push a new entry down the Echelon stack
func push(this Node, route []string, item *QueueEntry) error {
	if route[0] != this.Name() {
		log.Panicf("Unexpected echelon traversal: %s != %s", route[0], this.Name())
	}

	// End of the route
	if len(route) == 1 {
		return this.Push(item)
	}
	child := this.GetChild(route[1])
	if child == nil {
		child = this.NewChild(route[1])
	}
	return push(child, route[1:], item)
}

// Enqueue adds a set of objects to the queue. These objects must have fields corresponding to the returned
// list by InfoProvider.Keys (for instance [SourceSe, DestSe, Vo, Activity])
func (e *Echelon) Enqueue(item Item) error {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	err := push(
		e.root,
		append([]string{"/"}, item.GetPath()...),
		&QueueEntry{
			ID:        item.GetID(),
			Timestamp: item.GetTimestamp(),
		},
	)
	if err != nil {
		return err
	}

	return e.db.Put(item.GetID(), item)
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
func pickChild(children *[]string, weights *[]float32) (int, string) {
	// lock should be already acquired by caller
	ranges := recalculateRanges(weights)
	chance := rand.Float32()
	for index, child := range *children {
		top := ranges[index]
		if top >= chance {
			return index, child
		}
	}
	return -1, ""
}

// pop next element from the Echelon stack
func pop(this Node, provider InfoProvider, parent []string) (*QueueEntry, error) {
	route := append(parent, this.Name())

	// Leaf node
	if this.HasQueued() {
		available, err := provider.IsThereAvailableSlots(route[1:])
		if err != nil {
			return nil, err
		}
		if !available {
			return nil, ErrNotEnoughSlots
		}
		return this.Pop()
	}

	// Available slots for the path so far
	available, err := provider.IsThereAvailableSlots(route[1:])
	if err != nil {
		return nil, err
	} else if !available {
		// Nothing available, so do not even bother with recursion
		return nil, ErrNotEnoughSlots
	}

	// Intermediate node
	// We choose a random child based on their weight, and ask recursively
	// Since we may be unlucky enough to pick a path without available slots deeper down,
	// we iterate until we exhaust all possible children
	var selected Node
	var item *QueueEntry

	possibleChoices := this.ChildNames()
	weights := make([]float32, len(possibleChoices))
	childRoute := make([]string, len(route)+1)
	copy(childRoute, route)

	for index, child := range possibleChoices {
		childRoute[len(route)] = child
		weights[index] = provider.GetWeight(childRoute[1:])
	}

	for len(possibleChoices) > 0 {
		index, childName := pickChild(&possibleChoices, &weights)
		child := this.GetChild(childName)
		if child == nil {
			panic("Unexpected nil child")
		}

		item, err = pop(child, provider, route)
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
	if selected.Empty() {
		this.RemoveChild(selected.Name())
	}
	return item, nil
}

// Dequeue picks a single queued object from the queue tree. InfoProvider will be called to keep track of
// the available resources.
// If there are no available resources, or no enqueued items, this will return nil
// If the InfoProvider returns an error on any of its used methods, it will be propagated to the return value
// of this method.
func (e *Echelon) Dequeue(item interface{}) error {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	qi, err := pop(e.root, e.provider, []string{})
	if err != nil {
		return err
	}
	if err := e.db.Get(qi.ID, item); err != nil {
		return err
	}
	return e.db.Delete(qi.ID)
}
