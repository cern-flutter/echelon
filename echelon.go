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
	"reflect"
)

type (
	// InfoProvider is an interface used by Echelon to determine how to build the tree
	// and how to pick the elements.
	InfoProvider interface {
		// Keys must return an slice with the keys to be used to create the tree
		Keys() []string
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

	// Echelon contains a queue modeled like a tree where each child has a weight.
	// On the leaves, there will be an actual queue where FIFO is performed.
	Echelon struct {
		baseDir  string
		provider InfoProvider
		keys     []string
		root     *node
	}
)

var (
	// ErrInvalidKey is returned when an object is enqueued, but it doesn't have the expected
	// fields by Echelon (see InfoProvider.Keys)
	ErrInvalidKey = errors.New("Invalid key")
)

// New returns a new Echelon instance. The caller must pass an InfoProvider that will keep,
// if necessary, scoreboards, resource control, and/or the like.
func New(base string, provider InfoProvider) *Echelon {
	return &Echelon{
		baseDir:  base,
		provider: provider,
		keys:     provider.Keys(),
		root: &node{
			name:     "/",
			children: make([]*node, 0, 16),
			dir:      base,
		},
	}
}

// Close frees resources
func (e *Echelon) Close() {
	e.root.Close()
}

// String is a convenience method to generate a printable representation of the content of an
// Echelon instance.
func (e *Echelon) String() string {
	return fmt.Sprintf("Keys: %v\nQueue:\n%v", e.keys, e.root)
}

// getRouteForItem uses introspection to get the values associated with the required keys
// If the object doesn't have any of the required fields, it will return ErrInvalidKey)
func (e *Echelon) getRouteForItem(item interface{}) ([]string, error) {
	v := reflect.Indirect(reflect.ValueOf(item))
	values := make([]string, len(e.keys)+1)
	values[0] = "/"
	for index, key := range e.keys {
		field := v.FieldByName(key)
		if field.Kind() == reflect.Invalid {
			return nil, ErrInvalidKey
		}
		values[index+1] = field.String()
	}
	return values, nil
}

// Enqueue adss a set of objects to the queue. These objects must have fields corresponding to the returned
// list by InfoProvider.Keys (for instance [SourceSe, DestSe, Vo, Activity])
func (e *Echelon) Enqueue(items ...interface{}) error {
	for _, item := range items {
		route, err := e.getRouteForItem(item)
		if err != nil {
			return err
		}
		e.root.Push(e, route, item)
	}
	return nil
}

// Dequeue picks a single queued object from the queue tree. InfoProvider will be called to keep track of
// the available resources.
// If there are no available resources, or no enqueued items, this will return nil
// If the InfoProvider returns an error on any of its used methods, it will be propagated to the return value
// of this method.
func (e *Echelon) Dequeue(item interface{}) error {
	return e.root.Pop(item, e)
}
