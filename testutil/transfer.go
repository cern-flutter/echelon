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

package testutil

import (
	"github.com/satori/go.uuid"
	"time"
)

// Mock for testing
type Transfer struct {
	TransferId          string
	Source, Destination string
	SourceSe, DestSe    string
	Vo                  string
	Activity            string
	SubmitTime          time.Time
}

func (t *Transfer) GetID() string {
	return t.TransferId
}

func (t *Transfer) GetPath() []string {
	return []string{t.DestSe, t.Vo, t.Activity, t.SourceSe}
}

func (t *Transfer) GetTimestamp() time.Time {
	return t.SubmitTime
}

func GenerateRandomTransfer() *Transfer {
	sourceSe := RandomChoice(Storages)
	destSe := RandomChoice(Storages)
	file := RandomFile()

	return &Transfer{
		TransferId:  uuid.NewV4().String(),
		Source:      destSe + file,
		Destination: sourceSe + file,
		SourceSe:    sourceSe,
		DestSe:      destSe,
		Vo:          RandomChoice(Vos),
		Activity:    RandomChoice(Activities),
	}
}
