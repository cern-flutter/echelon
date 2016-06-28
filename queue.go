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
	"container/list"
)

type (
	// Queue provides a simple FIFO implementation
	Queue struct {
		list.List
	}
)

// Push adds a new element to the back of the queue.
func (q *Queue) Push(element interface{}) {
	q.PushBack(element)
}

// Pop removes an element from the front of the queue.
// It return nil if the queue is empty.
func (q *Queue) Pop() interface{} {
	if iter := q.Front(); iter != nil {
		return q.Remove(iter)
	}
	return nil
}
