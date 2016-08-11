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

import ()
import (
	"bytes"
	"encoding/gob"
	"github.com/boltdb/bolt"
)

// Restore rebuilds the tree representation from the data available on disk
func (e *Echelon) Restore(prototype Item) error {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	return e.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("items"))
		return bucket.ForEach(func(key, value []byte) error {
			buffer := bytes.NewBuffer(value)
			if err := gob.NewDecoder(buffer).Decode(prototype); err != nil {
				return err
			}
			return e.root.push(e, prototype.GetPath(), &queueItem{
				ID:        prototype.GetID(),
				Timestamp: prototype.GetTimestamp(),
			})
		})
	})
}
