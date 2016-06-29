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
	"fmt"
	"math/rand"
)

var Storages = []string{"gsiftp://dpm.cern.ch", "srm://castor.cern.ch", "gsiftp://dcache.pic.es"}
var Vos = []string{"atlas"}
var ActivitiesWeigths = map[string]float32{
	"t0":         0.7,
	"production": 0.5,
	"staging":    0.5,
	"express":    0.4,
	"user":       0.1,
	"default":    0.02,
}
var Activities = make([]string, 0, len(ActivitiesWeigths))
var Paths = []string{"/var/log", "/etc", "/tmp", "/scratch"}

func init() {
	for key := range ActivitiesWeigths {
		Activities = append(Activities, key)
	}
}

// RandomChoice returns a random entry in the slice
func RandomChoice(choices []string) string {
	return choices[rand.Intn(len(choices))]
}

// RandomFile returns a random file name
func RandomFile() string {
	path := RandomChoice(Paths)
	return fmt.Sprintf("%s/file.%d", path, rand.Intn(255))
}
