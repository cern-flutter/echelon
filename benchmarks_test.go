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
	"gitlab.cern.ch/flutter/echelon/testutil"
	"os"
	"syscall"
	"testing"
	"time"
)

func BenchmarkEchelonEnqueue(b *testing.B) {
	echelon := New(BasePath, &TestProvider{})
	defer echelon.Close()

	for i := 0; i < b.N; i++ {
		transfer := testutil.GenerateRandomTransfer()
		if err := echelon.Enqueue(transfer); err != nil {
			b.Fatal(err)
		}
	}

	// Clean
	b.StopTimer()
	os.RemoveAll(BasePath)
}

func BenchmarkEchelonDequeue(b *testing.B) {
	b.StopTimer()

	echelon := New(BasePath, &TestProvider{})
	defer echelon.Close()

	// Populate
	for i := 0; i < b.N; i++ {
		transfer := testutil.GenerateRandomTransfer()
		if err := echelon.Enqueue(transfer); err != nil {
			b.Fatal(err)
		}
	}

	// Dequeue
	b.StartTimer()
	transfer := &testutil.Transfer{}
	for i := 0; i < b.N; i++ {
		if err := echelon.Dequeue(transfer); err != nil {
			b.Fatal(err)
		} else if false {
			b.Fatal("Unexpected nil")
		}
	}

	// Clean
	b.StopTimer()
	os.RemoveAll(BasePath)
}

func BenchmarkEcheleonEnqueueConcurrent(b *testing.B) {
	if err := os.Mkdir(BasePath, 0755); err != nil && err.(*os.PathError).Err != syscall.EEXIST {
		b.Fatal(err)
	}
	fmt.Println("Starting", b.N)

	echelon := New(BasePath, &TestProvider{})
	defer echelon.Close()

	// Dequeue
	done := make(chan error)
	go func() {
		transfer := &testutil.Transfer{}
		for j := 0; j < b.N; {
			if err := echelon.Dequeue(transfer); err == ErrEmpty {
				fmt.Println("Empty", j)
				time.Sleep(10 * time.Millisecond)
				fmt.Println("Done sleeping")
			} else if err != nil {
				done <- err
				return
			} else {
				j++
			}
		}
		done <- nil
	}()

	// Queue
	fmt.Println("Producing", b.N)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		transfer := testutil.Transfer{}
		if err := echelon.Enqueue(transfer); err != nil {
			b.Fatal(err)
		}
	}

	// Clean
	b.StopTimer()
	fmt.Println("Done producing", b.N)
	if err := <-done; err != nil {
		b.Fatal(err)
	}
	fmt.Println("Done waiting")
	os.RemoveAll(BasePath)
}
