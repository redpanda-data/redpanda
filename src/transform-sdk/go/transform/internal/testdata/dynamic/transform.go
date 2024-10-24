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
	"bytes"
	"encoding/binary"
	"errors"
	"os"

	"github.com/redpanda-data/redpanda/src/transform-sdk/go/transform"
)

func main() {
	transform.OnRecordWritten(identityTransform)
}

var allocated bytes.Buffer

func identityTransform(e transform.WriteEvent, w transform.RecordWriter) error {
	key := string(e.Record().Key)
	switch key {
	case "poison":
		return errors.New("☠︎")
	case "bomb":
		panic("💥")
	case "print":
		b := e.Record().Value
		// Don't ignore short reads!
		_, err := os.Stdout.Write(b)
		return err
	case "loop":
		for {
		}
	case "allocate":
		amt := binary.LittleEndian.Uint32(e.Record().Value)
		allocated.Grow(int(amt))
		return nil
	case "zero":
		return nil
	case "mirror":
		return w.Write(e.Record())
	case "double":
		if err := w.Write(e.Record()); err != nil {
			return err
		}
		return w.Write(e.Record())
	}
	println("unknown record:", key)
	// omit nothing
	return nil
}
