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

package sr

import (
	"encoding/binary"

	"github.com/redpanda-data/redpanda/src/transform-sdk/go/transform/internal/rwbuf"
)

func decodeSchema(subject string, buf *rwbuf.RWBuf) (s SubjectSchema, err error) {
	s.Subject = subject
	id, err := binary.ReadVarint(buf)
	if err != nil {
		return s, err
	}
	s.ID = int(id)
	v, err := binary.ReadVarint(buf)
	if err != nil {
		return s, err
	}
	s.Version = int(v)
	s.Schema, err = decodeSchemaDef(buf)
	return
}

func decodeSchemaDef(buf *rwbuf.RWBuf) (s Schema, err error) {
	t, err := binary.ReadVarint(buf)
	if err != nil {
		return s, err
	}
	s.Type = SchemaType(t)
	s.Schema, err = buf.ReadSizedStringCopy()
	if err != nil {
		return s, err
	}
	rc, err := binary.ReadVarint(buf)
	if err != nil {
		return s, err
	}
	s.References = make([]Reference, rc)
	for i := int64(0); i < rc; i++ {
		s.References[i].Name, err = buf.ReadSizedStringCopy()
		if err != nil {
			return
		}
		s.References[i].Subject, err = buf.ReadSizedStringCopy()
		if err != nil {
			return
		}
		v, err := binary.ReadVarint(buf)
		if err != nil {
			return s, err
		}
		s.References[i].Version = int(v)
	}
	return
}

func encodeSchemaDef(buf *rwbuf.RWBuf, s Schema) {
	buf.WriteVarint(int64(s.Type))
	buf.WriteStringWithSize(s.Schema)
	if s.References != nil {
		buf.WriteVarint(int64(len(s.References)))
		for _, r := range s.References {
			buf.WriteStringWithSize(r.Name)
			buf.WriteStringWithSize(r.Subject)
			buf.WriteVarint(int64(r.Version))
		}
	} else {
		buf.WriteVarint(0)
	}
}
