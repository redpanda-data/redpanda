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

package rwbuf

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"testing"
)

func TestWriteThenRead(t *testing.T) {
	b := New(2048)
	if b.ReaderLen() != 0 {
		t.Error("invalid initial reader len:", b.ReaderLen())
	}
	if b.WriterLen() != 2048 {
		t.Error("invalid initial writer len:", b.WriterLen())
	}
	n, err := b.Write([]byte{1, 2, 3, 4})
	if n != 4 || err != nil {
		t.Error("invalid write response:", n, "err:", err)
	}
	if b.ReaderLen() != 4 {
		t.Error("invalid reader len after write:", b.ReaderLen())
	}
	s, err := b.ReadSlice(1)
	if err != nil {
		t.Error(err)
	}
	if !bytes.Equal(s, []byte{1}) {
		t.Error("invalid read of first byte", hex.EncodeToString(s))
	}
	b.Reset()
	if b.ReaderLen() != 0 {
		t.Error("invalid reader len after reset:", b.WriterLen())
	}
	if b.WriterLen() != 2048 {
		t.Error("invalid writer len after reset:", b.WriterLen())
	}
}

type example struct {
	A int
	B string
	C float32
}

func TestDelayWrite(t *testing.T) {
	b := New(2048)
	s := 0
	fn := b.DelayWrite(4, func(buf []byte) {
		binary.LittleEndian.PutUint32(buf, uint32(s))
	})
	d := example{
		A: 42,
		B: "foo",
		C: 3.14,
	}
	c, err := json.Marshal(&d)
	if err != nil {
		t.Error(err)
	}
	s, err = b.Write(c)
	if err != nil {
		t.Error(err)
	}
	if s != len(c) {
		t.Error("didn't write all the bytes", s, "!=", len(c))
	}
	fn()
	l := b.ReaderLen()
	if l != 4+len(c) {
		t.Error("didn't update the reader len correctly", s, "!=", len(c))
	}
	c, err = b.ReadSlice(4)
	if err != nil {
		t.Error(err)
	}
	a := binary.LittleEndian.Uint32(c)
	if int(a) != s {
		t.Error("size mismatch", a, "!=", s)
	}
	c, err = b.ReadSlice(int(a))
	if err != nil {
		t.Error(err)
	}
	var e example
	err = json.Unmarshal(c, &e)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(d, e) {
		t.Error(d, "!=", e)
	}
	if b.ReaderLen() != 0 {
		t.Error("Reader still had bytes left:", b.ReaderLen())
	}
}
