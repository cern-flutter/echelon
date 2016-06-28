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
	"container/list"
	"github.com/satori/go.uuid"
	"math/rand"
	"reflect"
	"testing"
	"time"
)

type (
	// Mock for testing
	Transfer struct {
		transferId          string
		source, destination string
		sourceSe, destSe    string
		vo                  string
		activity            string
	}

	TestProvider struct{}
)

func (t *TestProvider) Keys() []string {
	return []string{"destSe", "vo", "activity", "sourceSe"}
}

func (t *TestProvider) GetWeight(field, value string) float32 {
	switch field {
	case "activity":
		return 0.1
	case "vo":
		return 0.5
	default:
		return 1
	}
}

func (t *TestProvider) GetAvailableSlots(path []string) (int, error) {
	return 1, nil
}

func (t *TestProvider) ConsumeSlot(path []string) error {
	return nil
}

func TestSimple(t *testing.T) {
	N := 10

	echelon := New(&TestProvider{})
	for i := 0; i < N; i++ {
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
		if err := echelon.Enqueue(transfer); err != nil {
			t.Fatal(err)
		}
	}

	elements := list.List{}
	for {
		if item, err := echelon.Dequeue(); err != nil {
			t.Fatal(err)
		} else if item != nil {
			t.Log(item)
			elements.PushBack(item)
		} else {
			break
		}
	}
	if elements.Len() != N {
		t.Fatal("Expecting", N, "got", elements.Len())
	}
}

func TestRacy1(t *testing.T) {
	N := 100
	echelon := New(&TestProvider{})
	done := make(chan bool)
	f := func() {
		for i := 0; i < N; i++ {
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
			if err := echelon.Enqueue(transfer); err != nil {
				t.Fatal(err)
			}
		}
		done <- true
	}

	go f()
	go f()

	for count := 0; count < 2; count++ {
		_ = <-done
	}
}

func TestRacy2(t *testing.T) {
	N := 50
	echelon := New(&TestProvider{})
	done := make(chan bool)

	produced := make([]*Transfer, N)
	consumed := make(map[string]*Transfer)

	producer := func() {
		for i := 0; i < N; i++ {
			sourceSe := randomChoice(storages)
			destSe := randomChoice(storages)
			file := randomFile()

			transfer := &Transfer{
				transferId:  uuid.NewV4().String(),
				source:      destSe + file,
				destination: sourceSe + file,
				sourceSe:    sourceSe,
				destSe:      destSe,
				vo:          randomChoice(vos),
				activity:    randomChoice(activities),
			}

			// Some time before the event to queue arrives
			time.Sleep(time.Duration(rand.Intn(50)) * time.Millisecond)
			t.Log("+ Enqueue", i)
			if err := echelon.Enqueue(transfer); err != nil {
				t.Fatal(err)
			}
			produced[i] = transfer
			t.Log("- Enqueue", i)
		}
		t.Log("Producer done")
		done <- true
	}
	consumer := func() {
		for i := 0; i < N; {
			t.Log("+ Dequeue")
			if item, err := echelon.Dequeue(); err != nil {
				t.Fatal(err)
			} else if item != nil {
				transfer := item.(*Transfer)
				consumed[transfer.transferId] = transfer
				t.Log("- Dequeue", i)
				i++
			} else {
				t.Log("- Dequeue empty")
				// If we don't sleep, the other goroutine may not wake up
				// See http://blog.nindalf.com/how-goroutines-work/
				time.Sleep(10 * time.Millisecond)
			}
		}
		t.Log("Consumer done")
		done <- true
	}

	go producer()
	go consumer()

	for count := 0; count < 2; count++ {
		_ = <-done
	}

	if len(produced) != len(consumed) {
		t.Fatal("Expected equal length of produced and consumed", N)
	}

	for _, p := range produced {
		c := consumed[p.transferId]
		if c == nil {
			t.Fatal("Missing consumed transfer")
		}
		if !reflect.DeepEqual(*c, *p) {
			t.Fatal("Produced and consumed do not match")
		}
	}
}
