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
	"errors"
	"gitlab.cern.ch/flutter/go-dirq"
)

type (
	// Queue provides a simple FIFO implementation
	Queue struct {
		dir  string
		dirq *dirq.Dirq
	}
)

var (
	ErrEmpty          = errors.New("Empty queue")
	ErrNotEnoughSlots = errors.New("Not enough slots")
	ErrNilChild       = errors.New("Unexpected nil child")
)

// NewQueue creates a new queue
func NewQueue(dir string) (q *Queue, err error) {
	q = &Queue{dir: dir}
	q.dirq, err = dirq.New(dir)
	return
}

// Closes releases resources used by the queue
func (q *Queue) Close() {
	q.dirq.Purge()
	q.dirq.Close()
}

// Push adds a new element to the back of the queue.
// element must be gob-serializable
func (q *Queue) Push(element interface{}) (err error) {
	buffer := &bytes.Buffer{}
	encoder := gob.NewEncoder(buffer)
	if err = encoder.Encode(element); err != nil {
		return
	}
	return q.dirq.Produce(buffer.Bytes())
}

// Pop removes an element from the front of the queue.
// It return ErrEmpty if empty
func (q *Queue) Pop(element interface{}) (err error) {
	if serialized, err := q.dirq.ConsumeOne(); err != nil {
		return err
	} else if serialized == nil {
		return ErrEmpty
	} else {
		buffer := bytes.NewBuffer(serialized)
		err = gob.NewDecoder(buffer).Decode(element)
	}
	return
}

// Empty returns true if there are no elements queued
func (q *Queue) Empty() (bool, error) {
	return q.dirq.Empty()
}
