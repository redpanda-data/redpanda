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

// A buffer with a reader and writer index that gives access to the underlying array.
package rwbuf

import (
	"encoding/binary"
	"io"
)

type RWBuf struct {
	// Underlying resizeable buffer
	buf []byte
	// Reader index. r <= w
	r int
	// Writer index
	w int
}

func New(size int) *RWBuf {
	return &RWBuf{
		buf: make([]byte, size),
		r:   0,
		w:   0,
	}
}

func (r *RWBuf) EnsureSize(n int) {
	if len(r.buf) >= n {
		return
	}
	r.buf = append(r.buf, make([]byte, n-len(r.buf))...)
}

func (r *RWBuf) Reset() {
	r.r = 0
	r.w = 0
}

func (r *RWBuf) WriterBuf() []byte {
	return r.buf[r.w:]
}

func (r *RWBuf) WriterBufPtr() *byte {
	return &r.buf[r.w]
}

func (r *RWBuf) WriterLen() int {
	return len(r.buf) - r.w
}

func (r *RWBuf) EnsureWriterSpace(n int) {
	r.EnsureSize(r.w + n)
}

func (r *RWBuf) AdvanceWriter(n int) {
	r.w += n
}

func (r *RWBuf) WriteVarint(v int64) {
	r.EnsureWriterSpace(binary.MaxVarintLen64)
	r.w += binary.PutVarint(r.WriterBuf(), v)
}

func (r *RWBuf) Write(b []byte) (int, error) {
	r.EnsureWriterSpace(len(b))
	copy(r.WriterBuf(), b)
	r.w += len(b)
	return len(b), nil
}

func (r *RWBuf) WriteString(s string) (int, error) {
	// This is "safe" because we only copy out of this slice, never write to it.
	return r.Write(unsafeStringToBytes(s))
}

func (r *RWBuf) WriteByte(b byte) error {
	r.EnsureWriterSpace(1)
	r.buf[r.w] = b
	r.w++
	return nil
}

func (r *RWBuf) WriteBytesWithSize(b []byte) {
	if b == nil {
		r.WriteVarint(-1)
		return
	}
	r.WriteVarint(int64(len(b)))
	// This cannot fail
	_, _ = r.Write(b)
}

func (r *RWBuf) WriteStringWithSize(s string) {
	r.WriteVarint(int64(len(s)))
	// This cannot fail
	_, _ = r.WriteString(s)
}

// Delay a write of size n, that will be issued using the supplied function,
// it can be invoked at any time at a later time, even if buffers have
// been resized.
func (r *RWBuf) DelayWrite(n int, fn func(b []byte)) func() {
	r.EnsureWriterSpace(n)
	m := r.w
	r.w += n
	return func() {
		fn(r.buf[m:n])
	}
}

func (r *RWBuf) AdvanceReader(n int) {
	r.r += n
	if r.r > r.w {
		r.r = r.w
	}
}

func (r *RWBuf) ReaderLen() int {
	return r.w - r.r
}

func (r *RWBuf) ReadByte() (byte, error) {
	if r.ReaderLen() == 0 {
		return 0, io.EOF
	}
	c := r.buf[r.r]
	r.r++
	return c, nil
}

func (r *RWBuf) ReadSlice(n int) ([]byte, error) {
	if n > r.ReaderLen() {
		return nil, io.ErrShortBuffer
	}
	i := r.r
	r.r += n
	return r.buf[i:r.r], nil
}

func (r *RWBuf) ReadSizedSlice() ([]byte, error) {
	l, err := binary.ReadVarint(r)
	if err != nil {
		return nil, err
	}
	var v []byte = nil
	if l >= 0 {
		v, err = r.ReadSlice(int(l))
		if err != nil {
			return nil, err
		}
	}
	return v, nil
}

func (r *RWBuf) ReadSizedStringCopy() (string, error) {
	b, err := r.ReadSizedSlice()
	if err != nil {
		return "", err
	}
	return string(b), err
}

func (r *RWBuf) ReadAll() []byte {
	// Errors are impossible here
	b, _ := r.ReadSlice(r.ReaderLen())
	return b
}
