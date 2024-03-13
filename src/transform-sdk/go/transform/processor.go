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

package transform

import (
	"errors"
	"strconv"
	"time"
	"unsafe"

	"github.com/redpanda-data/redpanda/src/transform-sdk/go/transform/internal/rwbuf"
)

type batchHeader struct {
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

type writeEvent struct {
	record Record
}

func (e *writeEvent) Record() Record {
	return e.record
}

type recordWriter struct {
	outbuf *rwbuf.RWBuf
	optbuf *rwbuf.RWBuf
}

func (w *recordWriter) Write(r Record, opts ...WriteOpt) error {
	// Serialize the record
	w.outbuf.Reset()
	r.serializePayload(w.outbuf)
	b := w.outbuf.ReadAll()

	// Apply write options
	wo := writeOpts{}
	for _, opt := range opts {
		opt.apply(&wo)
	}

	// Do the write
	var amt int32
	if wo.topic == "" {
		// Directly write the record to the default output topic.
		amt = writeRecord(unsafe.Pointer(&b[0]), int32(len(b)))
	} else {
		// Serialize the options
		w.optbuf.Reset()
		wo.serialize(w.optbuf)
		o := w.optbuf.ReadAll()
		amt = writeRecordWithOptions(
			unsafe.Pointer(&b[0]),
			int32(len(b)),
			unsafe.Pointer(&o[0]),
			int32(len(o)),
		)
	}
	if int(amt) != len(b) {
		return errors.New("writing record failed with errno: " + strconv.Itoa(int(amt)))
	}
	return nil
}

// Cache a bunch of objects to not GC
var (
	currentHeader batchHeader  = batchHeader{}
	inbuf         *rwbuf.RWBuf = rwbuf.New(128)
	e             writeEvent
	w             recordWriter = recordWriter{rwbuf.New(128), rwbuf.New(32)}
)

// run our transformation loop
func process(userTransformFunction OnRecordWrittenCallback) {
	if userTransformFunction == nil {
		panic("Invalid configuration, there is a nil registered user transform function")
	}
	checkAbiVersion()
	for {
		processBatch(userTransformFunction)
	}
}

// process and transform a single batch
func processBatch(userTransformFunction OnRecordWrittenCallback) {
	bufSize := int(readBatchHeader(
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
	))
	if bufSize < 0 {
		panic("failed to read batch header errno: " + strconv.Itoa(bufSize))
	}

	for i := 0; i < int(currentHeader.recordCount); i++ {
		inbuf.Reset()
		inbuf.EnsureSize(bufSize)
		var timestamp int64
		amt := int(readNextRecord(
			unsafe.Pointer(&e.record.Attrs.attr),
			unsafe.Pointer(&timestamp),
			unsafe.Pointer(&e.record.Offset),
			unsafe.Pointer(inbuf.WriterBufPtr()),
			int32(bufSize)),
		)
		// Assign the timestamp value to the record
		e.record.Timestamp = time.UnixMilli(timestamp)
		inbuf.AdvanceWriter(amt)
		if amt < 0 {
			panic("reading record failed with errno: " + strconv.Itoa(amt) + " buffer size: " + strconv.Itoa(bufSize))
		}
		if err := e.record.deserializePayload(inbuf); err != nil {
			panic("deserializing record failed: " + err.Error())
		}
		if err := userTransformFunction(&e, &w); err != nil {
			panic("transforming record failed: " + err.Error())
		}
	}
}
