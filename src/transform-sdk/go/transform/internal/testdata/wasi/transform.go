// Copyright 2023 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"encoding/json"
	"math/rand"
	"os"
	"time"

	"github.com/redpanda-data/redpanda/src/transform-sdk/go/transform"
)

func main() {
	transform.OnRecordWritten(wasiTransform)
}

type WasiInfo struct {
	Args         []string
	Env          []string
	NowNanos     int64
	RandomNumber int
}

func wasiTransform(e transform.WriteEvent, w transform.RecordWriter) error {
	i := &WasiInfo{
		Args:         os.Args,
		Env:          os.Environ(),
		NowNanos:     time.Now().UnixNano(),
		RandomNumber: rand.Int(),
	}
	b, err := json.Marshal(i)
	if err != nil {
		return err
	}
	return w.Write(transform.Record{Value: b})
}
