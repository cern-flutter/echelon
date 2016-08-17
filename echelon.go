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
	// Iterator is used by the storage interface to iterate stored values.
	StorageIterator interface {
		Next() bool
		Key() string
		Object(object interface{}) error
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

	// Echelon contains a queue modeled like a tree where each child has a weight.
	// On the leaves, there will be an actual queue where FIFO is performed.
	Echelon struct {
		provider  InfoProvider
		keys      []string
		root      *node
		mutex     sync.RWMutex
		db        Storage
		prototype Item
	}
)

// New returns a new Echelon instance. The caller must pass an InfoProvider that will keep,
// if necessary, scoreboards, resource control, and/or the like.
func New(prototype Item, db Storage, provider InfoProvider) (*Echelon, error) {
	return &Echelon{
		prototype: prototype,
		db:        db,
		provider:  provider,
		root: &node{
			name:     "/",
			children: make([]*node, 0, 16),
		},
	}, nil
}

// Close frees resources
func (e *Echelon) Close() error {
	return e.db.Close()
}

// String is a convenience method to generate a printable representation of the content of an
// Echelon instance.
func (e *Echelon) String() string {
	e.mutex.RLock()
	defer e.mutex.RUnlock()
	return fmt.Sprintf("Keys: %v\nQueue:\n%v", e.keys, e.root)
}

// Enqueue adds a set of objects to the queue. These objects must have fields corresponding to the returned
// list by InfoProvider.Keys (for instance [SourceSe, DestSe, Vo, Activity])
func (e *Echelon) Enqueue(item Item) error {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	if err := e.root.push(e, item.GetPath(), &queueItem{
		ID:        item.GetID(),
		Timestamp: item.GetTimestamp(),
	}); err != nil {
		return err
	}

	return e.db.Put(item.GetID(), item)
}

// Dequeue picks a single queued object from the queue tree. InfoProvider will be called to keep track of
// the available resources.
// If there are no available resources, or no enqueued items, this will return nil
// If the InfoProvider returns an error on any of its used methods, it will be propagated to the return value
// of this method.
func (e *Echelon) Dequeue(item interface{}) error {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	qi, err := e.root.pop(e)
	if err != nil {
		return err
	}
	if err := e.db.Get(qi.ID, item); err != nil {
		return err
	}
	return e.db.Delete(qi.ID)
}
