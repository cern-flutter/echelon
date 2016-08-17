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

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/satori/go.uuid"
	"gitlab.cern.ch/flutter/echelon"
	"gitlab.cern.ch/flutter/echelon/testutil"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"sort"
	"time"
)

const (
	DefaultPerLink = 100
	DefaultPerSe   = 100
)

type (
	// SimulationConfig holds the number of slots per storage and link, and the activity shares
	SimulationConfig struct {
		Activities         map[string]float32
		SlotsAsSource      map[string]int
		SlotsAsDestination map[string]int
		SlotsPerLink       map[string]int
	}

	// SimulationChoices groups possible Storages, Vos and Activities to use on the transfer generation
	SimulationChoices struct {
		Vos        []string
		Storages   []string
		Activities []string
	}

	// SimulationProvider implements the InfoProvider for the Echelon
	SimulationProvider struct {
		Config  SimulationConfig
		Choices SimulationChoices
	}

	// QueueId identifies uniquely a simple FIFO queue
	QueueId struct {
		Source, Dest string
		Vo           string
		Activity     string
	}

	// QueueCount stores how many transfers were picked for the given queue id
	QueueCount struct {
		QueueId
		Count int
	}

	// QueueCountSlice is a convenience alias for an slice, so we can pass it to sort
	QueueCountSlice []QueueCount
)

// String returns the string representation of a QueueCount
func (q QueueCount) String() string {
	return fmt.Sprintf("%s\t%s\t%s\t%-15s\t%d", q.Source, q.Dest, q.Vo, q.Activity, q.Count)
}

// Len returns the length of a QueueCountSlice
func (q *QueueCountSlice) Len() int {
	return len(*q)
}

// Less returns true if q[i] < q[j]
// Used by sort
func (q *QueueCountSlice) Less(i, j int) bool {
	return (*q)[i].Count < (*q)[j].Count
}

// Swap interchanges q[i] with q[j]
// Used by sort
func (q *QueueCountSlice) Swap(i, j int) {
	(*q)[i], (*q)[j] = (*q)[j], (*q)[i]
}

// Keys returns the keys used for the Echelon.
// In this case, there are four levels: DestSe, Vo, Activity and SourceSe
// This means, for instance, that we must choose an Activity before the Source
func (i *SimulationProvider) Keys() []string {
	return []string{"DestSe", "Vo", "Activity", "SourceSe"}
}

// GetWeight returns the associated weight with the field with the given value
// For instance, "Activity" field, value "Express", has a different weight that "Activity"
// field, value "User"
func (i *SimulationProvider) GetWeight(route []string) float32 {
	// Activities do not have equal weights
	if len(route) == 4 {
		if value := i.Config.Activities[route[3]]; value != 0 {
			return value
		} else {
			return i.Config.Activities["default"]
		}
	}
	// Fair share for the rest
	return 1.0
}

// IsThereAvailableSlot returns how many slots for the given path there are
// It doesn't limit the overall system (root)
// It limits the second level based on the "Destination" slots
// It doesn't limit by Vo
// It doesn't limit per Activity
// It limits the fifth level based on "Source" *and* "Link" slots
func (i *SimulationProvider) IsThereAvailableSlots(route []string) (bool, error) {
	slots := 0
	switch len(route) {
	// Our side (root)
	case 1:
		slots = 1000
	// For destination
	case 2:
		dest := route[1]
		if value, ok := i.Config.SlotsAsDestination[dest]; ok {
			slots = value
		} else {
			slots = DefaultPerSe
			i.Config.SlotsAsDestination[dest] = slots
		}
	// For VO, we do not have limitations
	case 3:
		slots = 1000
	// For activities, we do not have limitations
	case 4:
		slots = 1000
	// For the full path, we do have a limitation per link and per storage
	// Pick the lower
	case 5:
		dest := route[1]
		source := route[4]
		link := source + " " + dest

		var limitSource, limitLink int
		var ok bool
		if limitSource, ok = i.Config.SlotsAsSource[source]; !ok {
			limitSource = DefaultPerSe
			i.Config.SlotsAsSource[source] = limitSource
		}
		if limitLink, ok = i.Config.SlotsPerLink[link]; !ok {
			limitLink = DefaultPerLink
			i.Config.SlotsPerLink[link] = limitLink
		}
		if limitSource < limitLink {
			slots = limitSource
		} else {
			slots = limitLink
		}
	default:
		return false, fmt.Errorf("Unexpected path length: %d", len(route))
	}
	return slots > 0, nil
}

// ConsumeSlots reduces by one the number of available slots
// For the meaning of path, see GetAvailableSlots
func (i *SimulationProvider) ConsumeSlot(path []string) error {
	switch len(path) {
	// For destination
	case 2:
		dest := path[1]
		i.Config.SlotsAsDestination[dest]--
		if i.Config.SlotsAsDestination[dest] < 0 {
			return fmt.Errorf("Tried to consume a slot on an exhausted destination: %s %d",
				dest, i.Config.SlotsAsDestination[dest])
		}
	// For source and link
	case 5:
		dest := path[1]
		source := path[4]
		link := source + " " + dest
		i.Config.SlotsAsSource[source]--
		i.Config.SlotsPerLink[link]--
		if i.Config.SlotsAsSource[source] < 0 {
			return fmt.Errorf("Tried to consume a slot on an exhausted source: %s %d",
				source, i.Config.SlotsAsSource[source])
		}
		if i.Config.SlotsPerLink[link] < 0 {
			return fmt.Errorf("Tried to consume a slot on an exhausted link: %s %d",
				link, i.Config.SlotsPerLink[link])
		}
	}
	return nil
}

// populate generates n random transfers (using the configured possible choices)
// and push them into the Echelon queue
func (s *SimulationProvider) populate(queue *echelon.Echelon, n int) {
	for i := 0; i < n; i++ {
		sourceSe := testutil.RandomChoice(s.Choices.Storages)
		destSe := testutil.RandomChoice(s.Choices.Storages)
		file := testutil.RandomFile()

		transfer := &testutil.Transfer{
			TransferId:  uuid.NewV4().String(),
			Source:      destSe + file,
			Destination: sourceSe + file,
			SourceSe:    sourceSe,
			DestSe:      destSe,
			Vo:          testutil.RandomChoice(s.Choices.Vos),
			Activity:    testutil.RandomChoice(s.Choices.Activities),
		}
		if err := queue.Enqueue(transfer); err != nil {
			log.Panic(err)
		}
	}
}

// run picks n transfers from the queue, or as many until we run out of slots (or transfers)
// It returns how many have been picked, and the count grouped by Queue
func (s *SimulationProvider) run(queue *echelon.Echelon, n int) (int, map[QueueId]int) {
	consumed := make(map[QueueId]int)

	var i int
	for i = 0; i < n; i++ {
		transfer := &testutil.Transfer{}
		if err := queue.Dequeue(transfer); err != nil {
			if err == echelon.ErrEmpty || err == echelon.ErrNotEnoughSlots {
				fmt.Println("Run out of available elements")
				break
			}
			log.Panic(err)
		}
		consumed[QueueId{
			transfer.SourceSe, transfer.DestSe,
			transfer.Vo, transfer.Activity,
		}]++
		s.ConsumeSlot(transfer.GetPath())
	}

	return i, consumed
}

// load deserializes simulation data in json format stored in path
func (s *SimulationProvider) load(path string) {
	if raw, err := ioutil.ReadFile(path); err != nil {
		log.Fatal(err)
	} else if err := json.Unmarshal(raw, s); err != nil {
		log.Fatal(err)
	}
}

// main is the entry point
func main() {
	rand.Seed(time.Now().UnixNano())

	generate := flag.Int("generate", 1000, "How many transfers to generate")
	pick := flag.Int("pick", 100, "How many transfers to pick")
	dump := flag.String("dump-queue", "", "Dump the remaining queue into this file")
	dbPath := flag.String("db", "/tmp/simulation.db", "Echelon DB path (default)")
	redisAddr := flag.String("redis", "", "Redis address")
	keepDb := flag.Bool("keep", false, "Keep the echelon path once the simulation is done")
	flag.Parse()

	if flag.NArg() == 0 {
		log.Fatal("Missing simulation data (i.e. atlas.json)")
	}
	simulation := &SimulationProvider{}
	simulation.load(flag.Arg(0))

	var db echelon.Storage
	var err error

	if *redisAddr != "" {
		db, err = echelon.NewRedis(*redisAddr)
		fmt.Println("Using Redis")
	} else {
		db, err = echelon.NewLevelDb(*dbPath)
		fmt.Println("Using LevelDB")
	}
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	queue, err := echelon.New(&testutil.Transfer{}, db, simulation)
	if err != nil {
		log.Fatal(err)
	}

	// Prepare queue
	simulation.populate(queue, *generate)
	fmt.Println("Produced", *generate)

	// Run simulation
	consumed, quadCount := simulation.run(queue, *pick)
	fmt.Println("Consumed", consumed)

	// Clean up
	if !*keepDb {
		if *redisAddr != "" {
			fmt.Println("Cleaning Redis")
			db2, _ := echelon.NewRedis(*redisAddr)
			_, err := db2.Pool.Get().Do("FLUSHALL")
			if err != nil {
				fmt.Println("Failed to clean the DB")
			}
		} else {
			fmt.Println("Cleaning LevelDB")
			os.RemoveAll(*dbPath)
		}
	}

	// We need to convert to a slice of QueueCount so we can sort
	all := make(QueueCountSlice, 0, *pick)
	for quad := range quadCount {
		count := quadCount[quad]
		all = append(all, QueueCount{QueueId: quad, Count: count})
	}

	// Print stats
	countAsSource := make(map[string]int)
	countAsDestination := make(map[string]int)
	countLink := make(map[string]int)

	fmt.Println("")
	sort.Sort(sort.Reverse(&all))
	for _, item := range all {
		countAsSource[item.Source] += item.Count
		countAsDestination[item.Dest] += item.Count
		countLink[item.Source+" "+item.Dest] += item.Count

		fmt.Println(item)
	}

	fmt.Println("\nStorage\tSource\tDestination")
	for key := range countAsSource {
		fmt.Printf("%s\t%d\t%d\n", key, countAsSource[key], countAsDestination[key])
	}

	fmt.Println("\nLink")
	for key := range countLink {
		fmt.Printf("%s\t%d\n", key, countLink[key])
	}

	// Dump the remaining queue
	if *dump != "" {
		if err := ioutil.WriteFile(*dump, []byte(queue.String()), 0775); err != nil {
			fmt.Println("Failed to dump the queue:", err)
		}
	}
}
