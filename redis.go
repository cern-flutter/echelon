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
)

type (
	RedisDbImpl struct {
		conn redis.Conn
	}

	RedisDbIterator struct {
		conn      redis.Conn
		cursor    int
		items     []string
		retrieved int
		done      bool
	}
)

func NewRedis(address string) (*RedisDbImpl, error) {
	conn, err := redis.Dial("tcp", address)
	if err != nil {
		return nil, err
	}
	return &RedisDbImpl{
		conn: conn,
	}, nil
}

func (rdb *RedisDbImpl) Close() error {
	return rdb.conn.Close()
}

// Clear removes all keys on the DB! Careful with it
func (rdb *RedisDbImpl) Clear() error {
	_, err := rdb.conn.Do("FLUSHALL")
	return err
}

func (rdb *RedisDbImpl) Put(key string, object interface{}) error {
	serialized := &bytes.Buffer{}
	encoder := gob.NewEncoder(serialized)
	if err := encoder.Encode(object); err != nil {
		return err
	}

	_, err := rdb.conn.Do("SET", key, serialized.String())
	return err
}

func (rdb *RedisDbImpl) Get(key string, object interface{}) error {
	value, err := redis.Bytes(rdb.conn.Do("GET", key))
	if err != nil {
		return err
	}
	buffer := bytes.NewBuffer(value)
	return gob.NewDecoder(buffer).Decode(object)
}

func (rdb *RedisDbImpl) Delete(key string) error {
	_, err := rdb.conn.Do("DEL", key)
	return err
}

func (rdb *RedisDbImpl) NewIterator() StorageIterator {
	return &RedisDbIterator{
		conn:   rdb.conn,
		cursor: 0,
	}
}

func (iter *RedisDbIterator) Next() bool {
	if len(iter.items) > 0 {
		iter.items = iter.items[1:]
	}
	if len(iter.items) == 0 && !iter.done {
		values, err := redis.Values(iter.conn.Do("SCAN", iter.cursor))
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

func (iter *RedisDbIterator) Key() string {
	return iter.items[0]
}

func (iter *RedisDbIterator) Object(object interface{}) error {
	value, err := redis.Bytes(iter.conn.Do("GET", iter.Key()))
	if err != nil {
		return err
	}
	buffer := bytes.NewBuffer(value)
	return gob.NewDecoder(buffer).Decode(object)
}
