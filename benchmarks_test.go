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
	"gitlab.cern.ch/flutter/echelon/testutil"
	"runtime"
	"testing"
	"time"
)

func BenchmarkEchelonEnqueue(b *testing.B) {
	echelon := newEchelon()
	defer echelon.Close()

	for i := 0; i < b.N; i++ {
		transfer := testutil.GenerateRandomTransfer()
		if err := echelon.Enqueue(transfer); err != nil {
			b.Fatal(err)
		}
	}

	// Clean
	b.StopTimer()
	clearEchelon()
}

func BenchmarkEchelonDequeue(b *testing.B) {
	b.StopTimer()

	echelon := newEchelon()
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
	clearEchelon()
}

func BenchmarkEcheleonEnqueueConcurrent(b *testing.B) {
	echelon := newEchelon()
	defer echelon.Close()

	// Dequeue
	done := make(chan error)
	go func() {
		transfer := &testutil.Transfer{}
		for j := 0; j < b.N; {
			if err := echelon.Dequeue(transfer); err == ErrEmpty {
				runtime.Gosched() // Yield
				time.Sleep(10 * time.Millisecond)
			} else if err != nil {
				done <- err
				return
			} else {
				j++
			}
		}
		done <- nil
		close(done)
	}()

	// Generate transfers
	b.StopTimer()
	transfers := make([]*testutil.Transfer, b.N)
	for i := 0; i < b.N; i++ {
		transfers[i] = testutil.GenerateRandomTransfer()
	}

	// Queue
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		if err := echelon.Enqueue(transfers[i]); err != nil {
			b.Fatal(err)
		}
	}

	// Clean
	b.StopTimer()
	if err := <-done; err != nil {
		b.Fatal(err)
	}
	clearEchelon()
}
