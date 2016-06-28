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
	"fmt"
	"math/rand"
	"testing"
)

var storages = []string{"srm://dpm.cern.ch", "gsiftp://dpm.cern.ch", "srm://castor.cern.ch", "gsiftp://castor.cern.ch"}
var vos = []string{"atlas", "cms", "lhcb", "dteam"}
var activities = []string{"default", "express", "production", "user", "test"}
var paths = []string{"/var/log", "/etc", "/tmp", "/scratch"}

func randomChoice(choices []string) string {
	return choices[rand.Intn(len(choices))]
}

func randomFile() string {
	path := randomChoice(paths)
	return fmt.Sprintf("%s/file.%d", path, rand.Intn(255))
}

func BenchmarkEchelonEnqueue(b *testing.B) {
	e := New(&TestProvider{})

	for i := 0; i < b.N; i++ {
		sourceSe := randomChoice(storages)
		destSe := randomChoice(storages)
		file := randomFile()

		transfer := &Transfer{
			source:      destSe + file,
			destination: sourceSe + file,
			sourceSe:    sourceSe,
			destSe:      destSe,
			vo:          randomChoice(vos),
			activity:    randomChoice(activities),
		}
		if err := e.Enqueue(transfer); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEchelonDequeue(b *testing.B) {
	b.StopTimer()

	e := New(&TestProvider{})
	// Populate
	for i := 0; i < b.N; i++ {
		sourceSe := randomChoice(storages)
		destSe := randomChoice(storages)
		file := randomFile()

		transfer := &Transfer{
			source:      destSe + file,
			destination: sourceSe + file,
			sourceSe:    sourceSe,
			destSe:      destSe,
			vo:          randomChoice(vos),
			activity:    randomChoice(activities),
		}
		if err := e.Enqueue(transfer); err != nil {
			b.Fatal(err)
		}
	}

	// Dequeue
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		if item, err := e.Dequeue(); err != nil {
			b.Fatal(err)
		} else if item == nil {
			b.Fatal("Unexpected nil")
		}
	}
}
