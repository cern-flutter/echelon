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

// Restore rebuilds the tree representation from the data available on disk
func (e *Echelon) Restore() error {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	iter := e.db.NewIterator()
	defer iter.Close()
	for iter.Next() {
		if err := iter.Object(e.prototype); err != nil {
			return err
		}

		err := e.root.push(e.prototype.GetPath(), &queueItem{
			ID:        e.prototype.GetID(),
			Timestamp: e.prototype.GetTimestamp(),
		})
		if err != nil {
			return err
		}
	}
	return nil
}
