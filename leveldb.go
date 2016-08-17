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
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
)

type (
	// LevelDb implements the key-value store interface required by Echelon backed by a local
	// leveldb database
	LevelDb struct {
		db *leveldb.DB
	}

	// LevelDbIterator wraps a leveldb iterator
	LevelDbIterator struct {
		iter iterator.Iterator
	}
)

// NewLevelDb returns a LevelDb instance
func NewLevelDb(path string) (*LevelDb, error) {
	db, err := leveldb.OpenFile(path, nil)
	if err != nil {
		return nil, err
	}
	return &LevelDb{
		db: db,
	}, nil
}

// Close releases the underlying leveldb database
func (ldb *LevelDb) Close() error {
	return ldb.db.Close()
}

// Put stores the object serialized under the given key
func (ldb *LevelDb) Put(key string, object interface{}) error {
	serialized := &bytes.Buffer{}
	encoder := gob.NewEncoder(serialized)
	if err := encoder.Encode(object); err != nil {
		return err
	}
	return ldb.db.Put([]byte(key), serialized.Bytes(), nil)
}

// Get gets the object stored under the given key
func (ldb *LevelDb) Get(key string, object interface{}) error {
	value, err := ldb.db.Get([]byte(key), nil)
	if err != nil {
		return err
	}
	buffer := bytes.NewBuffer(value)
	return gob.NewDecoder(buffer).Decode(object)
}

// Delete deletes the object under the given key
func (ldb *LevelDb) Delete(key string) error {
	return ldb.db.Delete([]byte(key), nil)
}

// NewIterator returns a new iterator
func (ldb *LevelDb) NewIterator() StorageIterator {
	return &LevelDbIterator{ldb.db.NewIterator(nil, nil)}
}

// Next reads the following item
func (iter *LevelDbIterator) Next() bool {
	return iter.iter.Next()
}

// Key returns the current item key
func (iter *LevelDbIterator) Key() string {
	return string(iter.iter.Key())
}

// Object returns the current object
func (iter *LevelDbIterator) Object(object interface{}) error {
	value := iter.iter.Value()
	buffer := bytes.NewBuffer(value)
	return gob.NewDecoder(buffer).Decode(object)
}

// Close does nothing for LevelDB
func (iter *LevelDbIterator) Close() {
}
