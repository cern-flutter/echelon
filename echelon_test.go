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
	"gitlab.cern.ch/flutter/echelon/testutil"
	"math/rand"
	"os"
	"reflect"
	"runtime"
	"testing"
	"time"
)

type (
	TestProvider struct{}
)

const (
	BasePath = "/tmp/echelon.db"
)

func (t *TestProvider) GetWeight(route []string) float32 {
	switch len(route) {
	// VO
	case 3:
		return 0.5
	// Activity
	case 4:
		return 0.1
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

	echelon, err := New(BasePath, &TestProvider{})
	if err != nil {
		t.Fatal(err)
	}
	defer echelon.Close()

	for i := 0; i < N; i++ {
		transfer := testutil.GenerateRandomTransfer()
		if err := echelon.Enqueue(transfer); err != nil {
			t.Fatal(err)
		}
	}

	elements := list.List{}
	transfer := &testutil.Transfer{}
	for {
		if err := echelon.Dequeue(&transfer); err != nil && err != ErrEmpty {
			t.Fatal(err)
		} else if err == ErrEmpty {
			break
		} else {
			t.Log(transfer)
			elements.PushBack(transfer)
		}
	}
	if elements.Len() != N {
		t.Fatal("Expecting", N, "got", elements.Len())
	}
}

func TestRacy1(t *testing.T) {
	N := 100
	echelon, err := New(BasePath, &TestProvider{})
	if err != nil {
		t.Fatal(err)
	}
	defer echelon.Close()

	done := make(chan bool)
	f := func() {
		for i := 0; i < N; i++ {
			transfer := testutil.GenerateRandomTransfer()
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

	// Clean up for next tests
	os.RemoveAll(BasePath)
}

func TestRacy2(t *testing.T) {
	N := 50
	echelon, err := New(BasePath, &TestProvider{})
	if err != nil {
		t.Fatal(err)
	}
	defer echelon.Close()

	done := make(chan bool)

	produced := make([]*testutil.Transfer, N)
	consumed := make(map[string]*testutil.Transfer)

	producer := func() {
		for i := 0; i < N; i++ {
			transfer := testutil.GenerateRandomTransfer()
			// Some time before the event to queue arrives
			time.Sleep(time.Duration(rand.Intn(50)) * time.Millisecond)
			if err := echelon.Enqueue(transfer); err != nil {
				t.Fatal(err)
			}
			produced[i] = transfer
		}
		t.Log("Producer done")
		done <- true
	}
	consumer := func() {
		for i := 0; i < N; {
			transfer := &testutil.Transfer{}
			if err := echelon.Dequeue(transfer); err != nil && err != ErrEmpty {
				t.Fatal(err)
			} else if err == nil {
				consumed[transfer.TransferId] = transfer
				i++
			} else {
				// If we don't sleep, the other goroutine may not wake up
				// See http://blog.nindalf.com/how-goroutines-work/
				time.Sleep(10 * time.Millisecond)
				runtime.Gosched()
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
		t.Fatal("Expected equal length of produced and consumed", N, len(produced), len(consumed))
	}

	for _, p := range produced {
		c := consumed[p.TransferId]
		if c == nil {
			t.Fatal("Missing consumed transfer")
		}
		if !reflect.DeepEqual(*c, *p) {
			t.Fatal("Produced and consumed do not match")
		}
	}
}

func TestRestore(t *testing.T) {
	N := 50
	e1, err := New(BasePath, &TestProvider{})
	if err != nil {
		t.Fatal(err)
	}

	produced := make([]*testutil.Transfer, N)

	for i := 0; i < N; i++ {
		transfer := testutil.GenerateRandomTransfer()
		if err := e1.Enqueue(transfer); err != nil {
			t.Fatal(e1)
		}
		produced[i] = transfer
	}

	if len(produced) != N {
		t.Fatal("Didn't produce the messages?")
	}

	e1.Close()

	// Open a new one, must be able to consume what was generated before
	e2, err := New(BasePath, &TestProvider{})
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()

	if err := e2.Restore(&testutil.Transfer{}); err != nil {
		t.Fatal(err)
	}

	consumed := make(map[string]*testutil.Transfer)

	for i := 0; i < N; i++ {
		transfer := &testutil.Transfer{}
		if err := e2.Dequeue(transfer); err != nil {
			t.Fatal(err)
		}
		consumed[transfer.TransferId] = transfer
	}

	for _, p := range produced {
		c := consumed[p.TransferId]
		if c == nil {
			t.Fatal("Missing consumed transfer")
		}
		if !reflect.DeepEqual(*c, *p) {
			t.Fatal("Produced and consumed do not match")
		}
	}
}

func TestFirstEmpty(t *testing.T) {
	echelon, err := New(BasePath, &TestProvider{})
	if err != nil {
		t.Fatal(err)
	}
	defer echelon.Close()

	transfer := &testutil.Transfer{
		TransferId: uuid.NewV4().String(),
	}

	if err := echelon.Dequeue(transfer); err != ErrEmpty {
		t.Fatal(err)
	}

	if err := echelon.Enqueue(transfer); err != nil {
		t.Fatal(err)
	}

	if err := echelon.Dequeue(transfer); err != nil {
		t.Fatal(err)
	}
}

func TestSecondEmpty(t *testing.T) {
	echelon, err := New(BasePath, &TestProvider{})
	if err != nil {
		t.Fatal(err)
	}
	defer echelon.Close()
	transfer := &testutil.Transfer{
		TransferId: uuid.NewV4().String(),
	}

	if err := echelon.Dequeue(transfer); err != ErrEmpty {
		t.Fatal(err)
	}

	if err := echelon.Enqueue(transfer); err != nil {
		t.Fatal(err)
	}

	if err := echelon.Dequeue(transfer); err != nil {
		t.Fatal(err)
	}
	if err := echelon.Dequeue(transfer); err != ErrEmpty {
		t.Fatal(err)
	}

	if err := echelon.Enqueue(transfer); err != nil {
		t.Fatal(err)
	}
	if err := echelon.Enqueue(transfer); err != nil {
		t.Fatal(err)
	}
	if err := echelon.Dequeue(transfer); err != nil {
		t.Fatal(err)
	}

}

func TestEmpty(t *testing.T) {
	os.RemoveAll(BasePath)
	transfer := &testutil.Transfer{}

	echelon, err := New(BasePath, &TestProvider{})
	if err != nil {
		t.Fatal(err)
	}
	defer echelon.Close()

	if err := echelon.Dequeue(transfer); err != ErrEmpty {
		t.Fatal(err)
	}
}

// Setup
func TestMain(m *testing.M) {
	os.RemoveAll(BasePath)
	os.Exit(m.Run())
}
