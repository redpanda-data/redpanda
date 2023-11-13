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

//go:build wasip1 || tinygo

package redpanda

import (
	"unsafe"
)

// These are the host functions that go allows for wasm functions that are imported.
// See: https://github.com/golang/go/issues/59149

// An imported function to ensure that the broker supports this ABI version.
//
//go:wasmimport redpanda_transform check_abi_version_1
func checkAbiVersion()

// readRecordHeader reads all the data from the batch header into memory.
//
// Returns maximum record size for the batch in bytes (which is useful
// when you want to allocate a single buffer for the entire batch).
//
//go:wasmimport redpanda_transform read_batch_header
func readBatchHeader(
	baseOffset unsafe.Pointer,
	recordCount unsafe.Pointer,
	partitionLeaderEpoch unsafe.Pointer,
	attributes unsafe.Pointer,
	lastOffsetDelta unsafe.Pointer,
	baseTimestamp unsafe.Pointer,
	maxTimestamp unsafe.Pointer,
	producerId unsafe.Pointer,
	producerEpoch unsafe.Pointer,
	baseSequence unsafe.Pointer,
) int32

// readRecord reads the next record in the current batch.
//
// In addition to the record metadata, into `buf` the serialized the "payload"
// of a record will be written.
// The payload is the key, value and headers as specified by kafka's serialized
// wire protocol. In particular, the following fields should be as included:
//
// keyLength: varint
// key: byte[]
// valueLen: varint
// value: byte[]
// Headers => [Header]
//
// Returns the amount that was written into `buf` on success, otherwise
// returns a negative number to indicate an error.
//
//go:wasmimport redpanda_transform read_next_record
func readNextRecord(
	attributes unsafe.Pointer,
	timestamp unsafe.Pointer,
	offset unsafe.Pointer,
	buf unsafe.Pointer,
	len int32,
) int32

// writeRecord writes a new record by copying the data pointed to.
//
// The `buf` here is expected to be the serialized the "payload" of a record
// as specified by kafka's serialized wire protocol. In particular, the following
// fields should be as included:
//
// keyLength: varint
// key: byte[]
// valueLen: varint
// value: byte[]
// Headers => [Header]
//
// The record metdata such as the total length, attributes, timestamp and offset
// should be omitted - the broker will handle adding that information as required.
//
// See: https://kafka.apache.org/documentation/#record for more information.
//
// Returns a negative number to indicate an error.
//
//go:wasmimport redpanda_transform write_record
func writeRecord(data unsafe.Pointer, length int32) int32
