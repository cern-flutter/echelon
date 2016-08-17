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
	LevelDbImpl struct {
		db *leveldb.DB
	}

	LevelDbIterator struct {
		iter iterator.Iterator
	}
)

func NewLevelDb(path string) (Storage, error) {
	db, err := leveldb.OpenFile(path, nil)
	if err != nil {
		return nil, err
	}
	return &LevelDbImpl{
		db: db,
	}, nil
}

func (ldb *LevelDbImpl) Close() error {
	return ldb.db.Close()
}

func (ldb *LevelDbImpl) Put(key string, object interface{}) error {
	serialized := &bytes.Buffer{}
	encoder := gob.NewEncoder(serialized)
	if err := encoder.Encode(object); err != nil {
		return err
	}
	return ldb.db.Put([]byte(key), serialized.Bytes(), nil)
}

func (ldb *LevelDbImpl) Get(key string, object interface{}) error {
	value, err := ldb.db.Get([]byte(key), nil)
	if err != nil {
		return err
	}
	buffer := bytes.NewBuffer(value)
	return gob.NewDecoder(buffer).Decode(object)
}

func (ldb *LevelDbImpl) Delete(key string) error {
	return ldb.db.Delete([]byte(key), nil)
}

func (ldb *LevelDbImpl) NewIterator() StorageIterator {
	return &LevelDbIterator{ldb.db.NewIterator(nil, nil)}
}

func (iter *LevelDbIterator) Next() bool {
	return iter.iter.Next()
}

func (iter *LevelDbIterator) Key() string {
	return string(iter.iter.Key())
}

func (iter *LevelDbIterator) Object(object interface{}) error {
	value := iter.iter.Value()
	buffer := bytes.NewBuffer(value)
	return gob.NewDecoder(buffer).Decode(object)
}
