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
	log "github.com/Sirupsen/logrus"
	"github.com/garyburd/redigo/redis"
	"strings"
	"time"
)

type (
	// RedisDb implements the key-value store interface required by Echelon backed by a Redis server
	RedisDb struct {
		Pool   *redis.Pool
		Prefix string
	}

	// RedisDbIterator wraps the scanning of the stored elements
	RedisDbIterator struct {
		conn      redis.Conn
		cursor    int
		items     []string
		retrieved int
		done      bool
		prefix    string
	}
)

// NewRedis returns a RedisDb instance
// Use prefixes to store the objects with a known prefix that will allow to tell them apart from other
// objects that could be on the database.
func NewRedis(address string, prefixes ...string) (*RedisDb, error) {
	prefix := ""
	if len(prefixes) > 0 {
		prefix = prefixes[0]
	}
	return &RedisDb{
		Prefix: prefix,
		Pool: &redis.Pool{
			Dial: func() (redis.Conn, error) {
				return redis.Dial("tcp", address)
			},
			TestOnBorrow: func(c redis.Conn, t time.Time) error {
				_, err := c.Do("PING")
				return err
			},
			MaxIdle:     1,
			MaxActive:   1,
			IdleTimeout: 60 * time.Second,
			Wait:        false,
		},
	}, nil
}

// Close releases the underlying connection pool
func (rdb *RedisDb) Close() error {
	return rdb.Pool.Close()
}

// Put stores the object serialized under the given key
func (rdb *RedisDb) Put(key string, object interface{}) error {
	serialized := &bytes.Buffer{}
	encoder := gob.NewEncoder(serialized)
	if err := encoder.Encode(object); err != nil {
		return err
	}

	conn := rdb.Pool.Get()
	defer conn.Close()
	_, err := conn.Do("SET", rdb.Prefix+key, serialized.String())
	return err
}

// Get gets the object stored under the given key
func (rdb *RedisDb) Get(key string, object interface{}) error {
	conn := rdb.Pool.Get()
	defer conn.Close()

	value, err := redis.Bytes(conn.Do("GET", rdb.Prefix+key))
	if err != nil {
		return err
	}
	buffer := bytes.NewBuffer(value)
	return gob.NewDecoder(buffer).Decode(object)
}

// Delete deletes the object under the given key
func (rdb *RedisDb) Delete(key string) error {
	conn := rdb.Pool.Get()
	defer conn.Close()
	_, err := conn.Do("DEL", rdb.Prefix+key)
	return err
}

// NewIterator returns a new iterator
func (rdb *RedisDb) NewIterator() StorageIterator {
	return &RedisDbIterator{
		conn:   rdb.Pool.Get(),
		cursor: 0,
		prefix: rdb.Prefix,
	}
}

// Next reads the following item
func (iter *RedisDbIterator) Next() bool {
	if len(iter.items) > 0 {
		iter.items = iter.items[1:]
	}
	if len(iter.items) == 0 && !iter.done {
		values, err := redis.Values(iter.conn.Do("SCAN", iter.cursor, "MATCH", iter.prefix+"*"))
		if err != nil {
			log.Error(err)
			return false
		}
		_, err = redis.Scan(values, &iter.cursor, &iter.items)
		if err != nil {
			log.Error(err)
			return false
		}
		iter.retrieved += len(iter.items)
		iter.done = iter.cursor == 0
	}
	return len(iter.items) > 0

}

// Key returns the current item key
func (iter *RedisDbIterator) Key() string {
	return strings.TrimPrefix(iter.items[0], iter.prefix)
}

// Object returns the current object
func (iter *RedisDbIterator) Object(object interface{}) error {
	value, err := redis.Bytes(iter.conn.Do("GET", iter.items[0]))
	if err != nil {
		return err
	}
	buffer := bytes.NewBuffer(value)
	return gob.NewDecoder(buffer).Decode(object)
}

// Close releases the redis connection
func (iter *RedisDbIterator) Close() {
	iter.conn.Close()
}
