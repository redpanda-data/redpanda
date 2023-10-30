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

	"github.com/redpanda-data/redpanda/src/go/transform-sdk/internal/rwbuf"
)

// Reusable slice of record headers
var incomingRecordHeaders []RecordHeader = nil

func readKV(b *rwbuf.RWBuf) ([]byte, []byte, error) {
	k, err := b.ReadSizedSlice()
	if err != nil {
		return nil, nil, err
	}
	v, err := b.ReadSizedSlice()
	if err != nil {
		return nil, nil, err
	}
	return k, v, err
}

func (r *Record) deserializePayload(b *rwbuf.RWBuf) error {
	k, v, err := readKV(b)
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
	incomingRecordHeaders = incomingRecordHeaders[:hc]
	for i := 0; i < int(hc); i++ {
		k, v, err := readKV(b)
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
	r.Headers = incomingRecordHeaders
	return nil
}

// Serialize this output record's payload (key, value, headers) into the buffer
func (r Record) serializePayload(b *rwbuf.RWBuf) {
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
}
