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
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	"sync"
	"time"
)

type (
	// InfoProvider is an interface used by Echelon to determine how to build the tree
	// and how to pick the elements.
	InfoProvider interface {
		// GetWeigth is called to determine the weight of the value within the given field
		// For instance, for field = activity and value = express, weight = 10
		GetWeight(route []string) float32
		// GetAvailableSlots must return how many slots there are available for the given path
		// (i.e. [/, destination], or [/, destination, vo, activity, source])
		// It is up to the provider do decide on how to calculate these (using all, part,
		// always a big enough number...)
		GetAvailableSlots(route []string) (int, error)
		// ConsumeSlot is called by Echelon to mark an available slot has been used.
		// It is up to the InfoProvider to account for this.
		// Echelon will *never* increase the available slots, since it doesn't know how.
		ConsumeSlot(route []string) error
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
		provider InfoProvider
		keys     []string
		root     *node
		mutex    sync.RWMutex
		db       *leveldb.DB
	}
)

// New returns a new Echelon instance. The caller must pass an InfoProvider that will keep,
// if necessary, scoreboards, resource control, and/or the like.
func New(base string, provider InfoProvider) (*Echelon, error) {
	db, err := leveldb.OpenFile(base, nil)
	if err != nil {
		return nil, err
	}
	return &Echelon{
		db:       db,
		provider: provider,
		root: &node{
			name:     "/",
			children: make([]*node, 0, 16),
		},
	}, nil
}

// Close frees resources
func (e *Echelon) Close() {
	e.db.Close()
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

	serialized := &bytes.Buffer{}
	encoder := gob.NewEncoder(serialized)
	if err := encoder.Encode(item); err != nil {
		return err
	}

	if err := e.root.push(e, item.GetPath(), &queueItem{
		ID:        item.GetID(),
		Timestamp: item.GetTimestamp(),
	}); err != nil {
		return err
	}

	return e.db.Put([]byte(item.GetID()), serialized.Bytes(), nil)
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

	value, err := e.db.Get([]byte(qi.ID), nil)
	if err != nil {
		return err
	}

	buffer := bytes.NewBuffer(value)
	if err := gob.NewDecoder(buffer).Decode(item); err != nil {
		return err
	}

	return e.db.Delete([]byte(qi.ID), nil)
}
