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

package redpanda

import (
	"time"
)

// OnRecordWritten registers a callback to be fired when a record is written to the input topic.
//
// OnRecordWritten should be called in a package's `main` function to register the transform function that will be applied.
func OnRecordWritten(fn OnRecordWrittenCallback) {
	process(fn)
}

// OnRecordWrittenCallback is a callback to transform records after a write event happens in the input topic.
type OnRecordWrittenCallback func(e WriteEvent) ([]Record, error)

// WriteEvent contains information about the write that took place,
// namely it contains the record that was written.
type WriteEvent interface {
	// Access the record associated with this event
	Record() Record
}

// Look at options/builder pattern for making TransformEvent, and make it a "private" interface

type writeEvent struct {
	record Record
}

func (e *writeEvent) Record() Record {
	return e.record
}

// Headers are optional key/value pairs that are passed along with
// records.
type RecordHeader struct {
	Key   []byte
	Value []byte
}

// Record is a record that has been written to Redpanda.
type Record struct {
	// Key is an optional field.
	Key []byte
	// Value is the blob of data that is written to Redpanda.
	Value []byte
	// Headers are client specified key/value pairs that are
	// attached to a record.
	Headers []RecordHeader
	// Attrs is the attributes of a record.
	//
	// Output records should leave these unset.
	Attrs RecordAttrs
	// The timestamp associated with this record.
	//
	// For output records this can be left unset as it will
	// always be the same value as the input record.
	Timestamp time.Time
	// The offset of this record in the partition.
	//
	// For output records this field is left unset,
	// as it will be set by Redpanda.
	Offset int64
}

type RecordAttrs struct {
	attr uint8
}
