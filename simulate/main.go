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
	log "github.com/Sirupsen/logrus"
	"github.com/satori/go.uuid"
	"gitlab.cern.ch/flutter/echelon"
	"gitlab.cern.ch/flutter/echelon/testutil"
	"io/ioutil"
	"math/rand"
	"os"
	"sort"
	"strings"
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
	if len(route) == 3 {
		if value := i.Config.Activities[route[2]]; value != 0 {
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
	case 0:
		slots = 1000
	// For destination
	case 1:
		dest := route[0]
		if value, ok := i.Config.SlotsAsDestination[dest]; ok {
			slots = value
		} else {
			slots = DefaultPerSe
			i.Config.SlotsAsDestination[dest] = slots
		}
	// For VO, we do not have limitations
	case 2:
		slots = 1000
	// For activities, we do not have limitations
	case 3:
		slots = 1000
	// For the full path, we do have a limitation per link and per storage
	// Pick the lower
	case 4:
		dest := route[0]
		source := route[3]
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
	log.Debugf("%d available slots for %s", slots, strings.Join(route, " -> "))
	return slots > 0, nil
}

// ConsumeSlots reduces by one the number of available slots
// For the meaning of path, see GetAvailableSlots
func (i *SimulationProvider) ConsumeSlot(path []string) error {
	log.Debug("Consume slot for ", strings.Join(path, "/"))

	// For destination
	dest := path[0]
	log.Debug("Destination: ", dest)
	i.Config.SlotsAsDestination[dest]--
	if i.Config.SlotsAsDestination[dest] < 0 {
		return fmt.Errorf("Tried to consume a slot on an exhausted destination: %s %d",
			dest, i.Config.SlotsAsDestination[dest])
	}
	// For source and link
	source := path[3]
	link := source + " " + dest
	log.Debug("Link: ", link)
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

// hertz formats the count/duration in hertz
func hertz(count int, duration time.Duration) string {
	freq := float64(count) / duration.Seconds()
	if freq >= 1000000 {
		return fmt.Sprintf("%.2f MHz", freq / float64(1000000))
	} else if freq >= 1000 {
		return fmt.Sprintf("%.2f kHz", freq / float64(1000))
	}
	return fmt.Sprint(freq, " Hz")
}

// main is the entry point
func main() {
	rand.Seed(time.Now().UnixNano())

	generate := flag.Int("generate", 1000, "How many transfers to generate")
	pick := flag.Int("pick", 100, "How many transfers to pick")
	dump := flag.String("dump-queue", "", "Dump the remaining queue into this file")
	dbPath := flag.String("db", "/tmp/simulation.db", "Echelon DB path (default)")
	sqlAddr := flag.String("sql", "", "SQL Address")
	redisAddr := flag.String("redis", "", "Redis address")
	keepDb := flag.Bool("keep", false, "Keep the echelon path once the simulation is done")
	debug := flag.Bool("debug", false, "Enable debug output")
	flag.Parse()

	if flag.NArg() == 0 {
		log.Fatal("Missing simulation data (i.e. atlas.json)")
	}
	if *debug {
		log.SetLevel(log.DebugLevel)
	}

	simulation := &SimulationProvider{}
	simulation.load(flag.Arg(0))

	var db echelon.Storage
	var ns echelon.NodeStorage
	var err error

	ns = &echelon.MemNodeStorage{}

	if *redisAddr != "" {
		var redisDb *echelon.RedisDb
		redisDb, err = echelon.NewRedis(*redisAddr)
		db = redisDb
		ns = redisDb
		log.Info("Using Redis")
	} else if *sqlAddr != "" {
		db, err = echelon.NewSQL(*sqlAddr)
		log.Info("Using SQL")
	} else {
		db, err = echelon.NewLevelDb(*dbPath)
		log.Info("Using LevelDB")
	}
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	queue, err := echelon.New(&testutil.Transfer{}, db, ns, simulation)
	if err != nil {
		log.Fatal(err)
	}

	// Prepare queue
	start := time.Now()
	simulation.populate(queue, *generate)
	end := time.Now()
	duration := end.Sub(start)
	log.Info("Produced ", *generate, " in ", duration, " (", hertz(*generate, duration), ")")

	// Run simulation
	start = time.Now()
	consumed, quadCount := simulation.run(queue, *pick)
	end = time.Now()
	duration = end.Sub(start)
	log.Info("Consumed ", consumed, " in ", duration, " (", hertz(*generate, duration), ")")

	// Clean up
	if !*keepDb {
		if *redisAddr != "" {
			log.Info("Cleaning Redis")
			db2, _ := echelon.NewRedis(*redisAddr)
			_, err := db2.Pool.Get().Do("FLUSHALL")
			if err != nil {
				log.Error(err)
			}
		} else if *sqlAddr != "" {
			log.Info("Cleaning SQL")
			db2, _ := echelon.NewSQL(*sqlAddr)
			_, err := db2.Db.Exec("DELETE FROM t_file")
			if err != nil {
				log.Error(err)
			}
		} else {
			log.Info("Cleaning LevelDB")
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
