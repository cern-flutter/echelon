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
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/mattn/go-sqlite3"
	"gitlab.cern.ch/flutter/echelon/testutil"
	"strings"
)

type (
	// SQLDb is a SQL backend for Echelon
	SQLDb struct {
		Db *sql.DB
	}

	// SQLDbIterator wraps the scanning of the stored elements
	SQLDbIterator struct {
		rows     *sql.Rows
		err      error
		transfer testutil.Transfer
	}
)

// NewSQL returns a SQL instance
func NewSQL(url string) (*SQLDb, error) {
	var err error

	parts := strings.Split(url, "://")
	driver, address := parts[0], parts[1]

	db := &SQLDb{}
	db.Db, err = sql.Open(driver, address)
	if err != nil {
		return nil, err
	}

	return db, db.createTables()
}

// Create tables if not exist
func (sql *SQLDb) createTables() error {
	_, err := sql.Db.Exec(
		`
CREATE TABLE IF NOT EXISTS t_file (
	transfer_id	CHAR(36) PRIMARY KEY,
	file_state	VARCHAR(16) NOT NULL,
	source		VARCHAR(512),
	destination	VARCHAR(512),
	source_se	VARCHAR(255),
	dest_se		VARCHAR(255),
	vo_name		VARCHAR(64),
	activity	VARCHAR(32),
	submit_time	TIMESTAMP NOT NULL
)
`)
	return err
}

// Close releases the underlying connection pool
func (sql *SQLDb) Close() error {
	return sql.Db.Close()
}

// Put stores the object serialized under the given key
func (sql *SQLDb) Put(key string, object interface{}) error {
	transfer := object.(*testutil.Transfer)

	_, err := sql.Db.Exec(
		"INSERT INTO t_file (transfer_id, file_state, source, destination, source_se, dest_se, vo_name, activity, submit_time) "+
			"VALUES ($1, 'SUBMITTED', $2, $3, $4, $5, $6, $7, $8)",
		transfer.TransferId,
		transfer.Source, transfer.Destination,
		transfer.SourceSe, transfer.DestSe,
		transfer.Vo,
		transfer.Activity,
		transfer.SubmitTime,
	)

	return err
}

// Get gets the object stored under the given key
func (sql *SQLDb) Get(key string, object interface{}) error {
	transfer := object.(*testutil.Transfer)
	row := sql.Db.QueryRow(
		"SELECT transfer_id, source, destination, source_se, dest_se, vo_name, activity, submit_time "+
			"FROM t_file WHERE transfer_id = $1", key,
	)
	return row.Scan(
		&transfer.TransferId, &transfer.Source, &transfer.Destination,
		&transfer.SourceSe, &transfer.DestSe,
		&transfer.Vo, &transfer.Activity, &transfer.SubmitTime,
	)
}

// Delete deletes the object under the given key
func (sql *SQLDb) Delete(key string) error {
	_, err := sql.Db.Exec("DELETE FROM t_file WHERE transfer_id = $1", key)
	return err
}

// NewIterator returns a new iterator
func (sql *SQLDb) NewIterator() StorageIterator {
	iter := &SQLDbIterator{}
	iter.rows, iter.err = sql.Db.Query(
		"SELECT transfer_id, source, destination, source_se, dest_se, vo_name, activity, submit_time " +
			"FROM t_file WHERE file_state = 'SUBMITTED'",
	)
	return iter
}

// Next reads the following item
func (iter *SQLDbIterator) Next() bool {
	if iter.err != nil {
		return false
	}
	if !iter.rows.Next() {
		return false
	}

	iter.rows.Scan(
		&iter.transfer.TransferId, &iter.transfer.Source, &iter.transfer.Destination,
		&iter.transfer.SourceSe, &iter.transfer.DestSe,
		&iter.transfer.Vo, &iter.transfer.Activity, &iter.transfer.SubmitTime,
	)

	return true
}

// Key returns the current item key
func (iter *SQLDbIterator) Key() string {
	return iter.transfer.TransferId
}

// Object returns the current object
func (iter *SQLDbIterator) Object(object interface{}) error {
	dst := object.(*testutil.Transfer)
	*dst = iter.transfer
	return nil
}

// Close releases the redis connection
func (iter *SQLDbIterator) Close() {
	iter.rows.Close()
}
