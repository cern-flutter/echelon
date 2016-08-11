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
	"time"
)

type (
	queueItem struct {
		ID        string
		Timestamp time.Time
		Priority  int
	}

	queue []*queueItem
)

// NewQueue returns a new empty queue
func NewQueue() queue {
	return make(queue, 0, 50)
}

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
	item := x.(*queueItem)
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
