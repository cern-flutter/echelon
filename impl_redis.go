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
	"github.com/dustin/gojson"
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

	RedisNode struct {
		id, parent, name string
		db               *RedisDb
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
			MaxActive:   2,
			IdleTimeout: 60 * time.Second,
			Wait:        false,
		},
	}, nil
}

// Close releases the underlying connection pool
func (rdb *RedisDb) Close() error {
	return rdb.Pool.Close()
}

func (rdb *RedisDb) itemKey(key string) string {
	return rdb.Prefix + "-item-" + key
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
	_, err := conn.Do("SET", rdb.itemKey(key), serialized.String())
	return err
}

// Get gets the object stored under the given key
func (rdb *RedisDb) Get(key string, object interface{}) error {
	conn := rdb.Pool.Get()
	defer conn.Close()

	value, err := redis.Bytes(conn.Do("GET", rdb.itemKey(key)))
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
	_, err := conn.Do("DEL", rdb.itemKey(key))
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
		values, err := redis.Values(iter.conn.Do("SCAN", iter.cursor, "MATCH", iter.prefix+"-item-*"))
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

func (rdb *RedisDb) Root() Node {
	root := &RedisNode{
		db:   rdb,
		id:   "/",
		name: "/",
	}
	return root
}

func (n *RedisNode) childrenSetKey() string {
	return n.db.Prefix + "-node-" + n.id
}

func (n *RedisNode) queueKey() string {
	return n.db.Prefix + "-node-" + n.id + "-queue"
}

func (n *RedisNode) String() string {
	return n.name
}

func (n *RedisNode) Name() string {
	return n.name
}

func (n *RedisNode) NewChild(name string) (Node, error) {
	newNode := &RedisNode{
		db:     n.db,
		parent: n.id,
		id:     n.id + "-" + name,
		name:   name,
	}

	conn := n.db.Pool.Get()
	defer conn.Close()

	if _, err := conn.Do("SADD", n.childrenSetKey(), name); err != nil {
		return nil, err
	}

	return newNode, nil
}

func (n *RedisNode) GetChild(name string) (Node, error) {
	conn := n.db.Pool.Get()
	defer conn.Close()

	isMember, err := redis.Int(conn.Do("SISMEMBER", n.childrenSetKey(), name))
	if err != nil {
		return nil, err
	} else if isMember == 0 {
		return nil, ErrNotFound
	}

	return &RedisNode{
		db:     n.db,
		parent: n.id,
		id:     n.id + "-" + name,
		name:   name,
	}, nil
}

func (n *RedisNode) RemoveChild(target Node) error {
	conn := n.db.Pool.Get()
	defer conn.Close()

	child := target.(*RedisNode)

	if _, err := conn.Do("DEL", child.childrenSetKey()); err != nil {
		return err
	}

	if _, err := conn.Do("DEL", child.queueKey()); err != nil {
		return err
	}

	removed, err := redis.Int(conn.Do("SREM", n.childrenSetKey(), target.Name()))
	if err == nil && removed == 0 {
		err = ErrNotFound
	}
	return err
}

func (n *RedisNode) ChildNames() ([]string, error) {
	conn := n.db.Pool.Get()
	defer conn.Close()

	return redis.Strings(conn.Do("SMEMBERS", n.childrenSetKey()))
}

func (n *RedisNode) Empty() (bool, error) {
	conn := n.db.Pool.Get()
	defer conn.Close()

	childrenCount, err := redis.Int(conn.Do("SCARD", n.childrenSetKey()))
	if err != nil {
		return true, err
	}
	queuedCount, err := redis.Int(conn.Do("LLEN", n.queueKey()))
	if err != nil {
		return true, err
	}

	return childrenCount == 0 && queuedCount == 0, nil
}

func (n *RedisNode) HasQueued() (bool, error) {
	conn := n.db.Pool.Get()
	defer conn.Close()

	queuedCount, err := redis.Int(conn.Do("LLEN", n.queueKey()))
	return queuedCount > 0, err
}

func (n *RedisNode) Push(entry *QueueEntry) error {
	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	conn := n.db.Pool.Get()
	defer conn.Close()

	_, err = conn.Do("LPUSH", n.queueKey(), data)
	return err
}

func (n *RedisNode) Pop() (*QueueEntry, error) {
	conn := n.db.Pool.Get()
	defer conn.Close()

	data, err := redis.Bytes(conn.Do("LPOP", n.queueKey()))
	if err != nil {
		return nil, err
	}

	entry := QueueEntry{}
	err = json.Unmarshal(data, &entry)
	return &entry, err
}
