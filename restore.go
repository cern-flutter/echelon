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
	"net/url"
	"os"
	"path"
)

// Restore rebuilds the tree representation from the data available on disk
func (e *Echelon) Restore() error {
	e.mutex.Lock()
	defer e.mutex.Unlock()
	return e.root.restoreRecursive(e, make([]string, 0))
}

// restoreRecursive rebuilds recursively the tree
func (n *node) restoreRecursive(e *Echelon, parent []string) error {
	route := append(parent, n.name)

	// Leaf node
	// The +1 is to account for '/'
	if len(route) == len(e.keys)+1 {
		var err error
		n.queue, err = NewQueue(n.dir)
		return err
	}

	// Intermediate levels
	n.children = make([]*node, 0)

	fd, err := os.Open(n.dir)
	if err != nil {
		return err
	}
	defer fd.Close()

	for entry, err := fd.Readdir(1); err == nil && entry != nil; entry, err = fd.Readdir(1) {
		name, err := url.QueryUnescape(entry[0].Name())
		if err != nil {
			return err
		}
		child := &node{
			name: name,
			dir:  path.Join(n.dir, entry[0].Name()),
		}
		n.children = append(n.children, child)
		child.restoreRecursive(e, route)
	}
	if err != nil {
		return err
	}

	return nil
}
