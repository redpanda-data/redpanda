// Copyright 2023 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package redpanda

import (
	"time"
	"unsafe"

	"github.com/redpanda-data/redpanda/src/go/transform-sdk/internal/rwbuf"
)

type (
	eventErrorCode    int32
	inputBatchHandle  int32
	inputRecordHandle int32
)

const (
	evtSuccess       = eventErrorCode(0)
	evtConfigError   = eventErrorCode(1)
	evtUserError     = eventErrorCode(2)
	evtInternalError = eventErrorCode(3)
)

type batchHeader struct {
	handle               inputBatchHandle
	baseOffset           int64
	recordCount          int
	partitionLeaderEpoch int
	attributes           int16
	lastOffsetDelta      int
	baseTimestamp        int64
	maxTimestamp         int64
	producerId           int64
	producerEpoch        int16
	baseSequence         int
}

// Cache a bunch of objects to not GC
var (
	currentHeader batchHeader  = batchHeader{handle: -1}
	inbuf         *rwbuf.RWBuf = rwbuf.New(128)
	outbuf        *rwbuf.RWBuf = rwbuf.New(128)
	e             writeEvent
)

// The ABI that our SDK provides. Redpanda executes this function to determine the protocol contract to execute.
//
//lint:ignore U1000 this is not unused, but exposed for our Wasm host environment
//export redpanda_transform_abi_version
func redpandaAbiVersion() int32 {
	return 1
}

// The Redpanda broker will invoke this method for each record that needs to be transformed.
//
//lint:ignore U1000 this is not unused, but exposed for our Wasm host environment
//export redpanda_transform_on_record_written
func redpandaOnRecord(bh inputBatchHandle, rh inputRecordHandle, recordSize int, currentRelativeOutputOffset int) eventErrorCode {
	if userTransformFunction == nil {
		println("Invalid configuration, there is no registered user transform function")
		return evtConfigError
	}
	if currentHeader.handle != bh {
		currentHeader.handle = bh
		errno := readRecordHeader(
			bh,
			unsafe.Pointer(&currentHeader.baseOffset),
			unsafe.Pointer(&currentHeader.recordCount),
			unsafe.Pointer(&currentHeader.partitionLeaderEpoch),
			unsafe.Pointer(&currentHeader.attributes),
			unsafe.Pointer(&currentHeader.lastOffsetDelta),
			unsafe.Pointer(&currentHeader.baseTimestamp),
			unsafe.Pointer(&currentHeader.maxTimestamp),
			unsafe.Pointer(&currentHeader.producerId),
			unsafe.Pointer(&currentHeader.producerEpoch),
			unsafe.Pointer(&currentHeader.baseSequence),
		)
		if errno != 0 {
			println("Failed to read batch header")
			return evtInternalError
		}
	}
	// TODO: Also release memory if a single large record comes in that is bigger than "normal" records.
	inbuf.Reset()
	inbuf.EnsureSize(recordSize)
	amt := int(readRecord(rh, unsafe.Pointer(inbuf.WriterBufPtr()), int32(recordSize)))
	inbuf.AdvanceWriter(amt)
	if amt != recordSize {
		println("reading record failed with errno:", amt)
		return evtInternalError
	}
	err := e.record.deserialize(inbuf)
	if err != nil {
		println("deserializing record failed:", err.Error())
		return evtInternalError
	}
	// Save the original timestamp for output records
	ot := e.Record().Timestamp
	// Fix up the offsets to be absolute values
	e.record.Offset += currentHeader.baseOffset
	if e.record.Attrs.TimestampType() == 0 {
		e.record.Timestamp = time.UnixMilli(e.record.Timestamp.UnixMilli() + currentHeader.baseTimestamp)
	} else {
		e.record.Timestamp = time.UnixMilli(currentHeader.maxTimestamp)
	}
	rs, err := userTransformFunction(&e)
	if err != nil {
		println("transforming record failed:", err.Error())
		return evtUserError
	}
	if rs == nil {
		return evtSuccess
	}
	for i, r := range rs {
		// Because the previous record in the batch could have
		// output multiple records, we need to account for this,
		// by adjusting the offset accordingly.
		r.Offset = int64(currentRelativeOutputOffset + i)
		// Keep the same timestamp as the input record.
		r.Timestamp = ot

		outbuf.Reset()
		r.serialize(outbuf)
		b := outbuf.ReadAll()
		amt := int(writeRecord(unsafe.Pointer(&b[0]), int32(len(b))))
		if amt != len(b) {
			println("writing record failed with errno:", amt)
			return evtInternalError
		}
	}
	return evtSuccess
}
