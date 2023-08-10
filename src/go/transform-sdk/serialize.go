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
	"encoding/binary"
	"errors"
	"time"

	"github.com/redpanda-data/redpanda/src/go/transform-sdk/internal/rwbuf"
)

// Reusable slice of record headers
var incomingRecordHeaders []RecordHeader = nil

func (r *Record) deserialize(b *rwbuf.RWBuf) error {
	rs, err := binary.ReadVarint(b)
	if err != nil {
		return err
	}
	if rs != int64(b.ReaderLen()) {
		return errors.New("record size mismatch")
	}
	attr, err := b.ReadByte()
	if err != nil {
		return err
	}
	td, err := binary.ReadVarint(b)
	if err != nil {
		return err
	}
	od, err := binary.ReadVarint(b)
	if err != nil {
		return err
	}

	k, err := b.ReadSizedSlice()
	if err != nil {
		return err
	}
	v, err := b.ReadSizedSlice()
	if err != nil {
		return err
	}
	hc, err := binary.ReadVarint(b)
	if err != nil {
		return err
	}
	if incomingRecordHeaders == nil || len(incomingRecordHeaders) < int(hc) {
		incomingRecordHeaders = make([]RecordHeader, hc)
	}
	for i := 0; i < int(hc); i++ {
		k, err := b.ReadSizedSlice()
		if err != nil {
			return err
		}
		v, err := b.ReadSizedSlice()
		if err != nil {
			return err
		}
		incomingRecordHeaders[i] = RecordHeader{
			Key:   k,
			Value: v,
		}
	}
	r.Key = k
	r.Value = v
	r.Attrs.attr = attr
	r.Headers = incomingRecordHeaders
	r.Timestamp = time.UnixMilli(td)
	r.Offset = od
	return nil
}

// Serialize this output record into the buffer
func (r Record) serialize(b *rwbuf.RWBuf) {
	// The first thing we write is the size of the record,
	// which we need to go over the data to see how big that is.
	// Instead of going over the data twice, we reserve some space
	// then write the varint at the end in the space we reserved.
	f := b.DelayWrite(binary.MaxVarintLen32, func(buf []byte) {
		rs := int64(b.ReaderLen() - binary.MaxVarintLen32)
		o := binary.PutVarint(buf, rs)
		// Shift the varint to the start of the record
		copy(buf[len(buf)-o:], buf[:o])
		// Skip over the bytes we didn't use
		b.AdvanceReader(len(buf) - o)
	})
	// this can never fail
	_ = b.WriteByte(r.Attrs.attr)
	b.WriteVarint(r.Timestamp.UnixMilli())
	b.WriteVarint(r.Offset)
	b.WriteBytesWithSize(r.Key)
	b.WriteBytesWithSize(r.Value)
	if r.Headers != nil {
		b.WriteVarint(int64(len(r.Headers)))
		for _, h := range r.Headers {
			b.WriteBytesWithSize(h.Key)
			b.WriteBytesWithSize(h.Value)
		}
	} else {
		b.WriteVarint(0)
	}
	// Now write the header
	f()
}
