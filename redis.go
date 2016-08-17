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
	"time"
)

type (
	RedisDb struct {
		Pool *redis.Pool
	}

	RedisDbIterator struct {
		conn      redis.Conn
		cursor    int
		items     []string
		retrieved int
		done      bool
	}
)

func NewRedis(address string) (*RedisDb, error) {
	return &RedisDb{
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

func (rdb *RedisDb) Close() error {
	return rdb.Pool.Close()
}

// Clear removes all keys on the DB! Careful with it
func (rdb *RedisDb) Clear() error {
	conn := rdb.Pool.Get()
	defer conn.Close()
	_, err := conn.Do("FLUSHALL")
	return err
}

func (rdb *RedisDb) Put(key string, object interface{}) error {
	serialized := &bytes.Buffer{}
	encoder := gob.NewEncoder(serialized)
	if err := encoder.Encode(object); err != nil {
		return err
	}

	conn := rdb.Pool.Get()
	defer conn.Close()
	_, err := conn.Do("SET", key, serialized.String())
	return err
}

func (rdb *RedisDb) Get(key string, object interface{}) error {
	conn := rdb.Pool.Get()
	defer conn.Close()

	value, err := redis.Bytes(conn.Do("GET", key))
	if err != nil {
		return err
	}
	buffer := bytes.NewBuffer(value)
	return gob.NewDecoder(buffer).Decode(object)
}

func (rdb *RedisDb) Delete(key string) error {
	conn := rdb.Pool.Get()
	defer conn.Close()
	_, err := conn.Do("DEL", key)
	return err
}

func (rdb *RedisDb) NewIterator() StorageIterator {
	return &RedisDbIterator{
		conn:   rdb.Pool.Get(),
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

func (iter *RedisDbIterator) Close() {
	iter.conn.Close()
}
