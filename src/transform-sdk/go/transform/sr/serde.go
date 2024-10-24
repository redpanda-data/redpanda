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

package sr

import (
	"encoding/binary"
	"errors"
	"fmt"
)

var (
	// ErrNotRegistered is returned from Serde when attempting to encode a
	// value or decode an ID that has not been registered, or when using
	// Decode with a missing new value function.
	ErrNotRegistered = errors.New("registration is missing for encode/decode")

	// ErrBadHeader is returned from Decode when the input slice is shorter
	// than five bytes, or if the first byte is not the magic 0 byte.
	ErrBadHeader = errors.New("5 byte header for value is missing or does not have the 0 magic byte")
)

type (
	// SerdeOpt is an option to configure a Serde.
	SerdeOpt[T any] interface{ apply(*idSerde[T]) }
	serdeOpt[T any] struct{ fn func(*idSerde[T]) }
)

//lint:ignore U1000 this is required to adhere to the SerdeOpt interface
func (o serdeOpt[T]) apply(t *idSerde[T]) { o.fn(t) }

// EncodeFn allows Serde to encode a value.
func EncodeFn[T any](fn func(T) ([]byte, error)) SerdeOpt[T] {
	return serdeOpt[T]{func(t *idSerde[T]) { t.encode = fn }}
}

// AppendEncodeFn allows Serde to encode a value to an existing slice. This
// can be more efficient than EncodeFn; this function is used if it exists.
func AppendEncodeFn[T any](fn func([]byte, T) ([]byte, error)) SerdeOpt[T] {
	return serdeOpt[T]{func(t *idSerde[T]) { t.appendEncode = fn }}
}

// DecodeFn allows Serde to decode into a value.
func DecodeFn[T any](fn func([]byte, T) error) SerdeOpt[T] {
	return serdeOpt[T]{func(t *idSerde[T]) { t.decode = fn }}
}

func keySubject[T any](base_name string, fn func(string) error) SerdeOpt[T] {
	return serdeOpt[T]{func(t *idSerde[T]) {
		t.ks_err = fn(fmt.Sprintf("%s-key", base_name))
	}}
}

// KeySubjectTopicName tells Serde to construct a subject name using topic name strategy and
// pass the result to a user-supplied function, where, for example, the subject might be pushed
// out to schema registry. If the user supplied function returns an error, subsequent call to
// Serde.Encode will short circuit, returning that error.
func KeySubjectTopicName[T any](topic string, fn func(string) error) SerdeOpt[T] {
	return keySubject[T](topic, fn)
}

// KeySubjectRecordName tells Serde to construct a subject name using record name strategy and
// pass the result to a user-supplied function, where, for example, the subject might be pushed
// out to schema registry. If the user supplied function returns an error, subsequent call to
// Serde.Encode will short circuit, returning that error.
func KeySubjectRecordName[T any](record_name string, fn func(string) error) SerdeOpt[T] {
	return keySubject[T](record_name, fn)
}

// KeySubjectTopicRecordName tells Serde to construct a subject name using topic record name
// strategy and pass the result to a user-supplied function, where, for example, the subject
// might be pushed out to schema registry. If the user supplied function returns an error,
// subsequent call to Serde.Encode will short circuit, returning that error.
func KeySubjectTopicRecordName[T any](topic string, record_name string, fn func(string) error) SerdeOpt[T] {
	return keySubject[T](fmt.Sprintf("%s-%s", topic, record_name), fn)
}

func valueSubject[T any](base_name string, fn func(string) error) SerdeOpt[T] {
	return serdeOpt[T]{func(t *idSerde[T]) {
		t.vs_err = fn(fmt.Sprintf("%s-value", base_name))
	}}
}

// ValueSubjectTopicName tells Serde to construct a subject name using topic name strategy and
// pass the result to a user-supplied function, where, for example, the subject might be pushed
// out to schema registry. If the user supplied function returns an error, subsequent call to
// Serde.Encode will short circuit, returning that error.
func ValueSubjectTopicName[T any](topic string, fn func(string) error) SerdeOpt[T] {
	return valueSubject[T](topic, fn)
}

// ValueSubjectRecordName tells Serde to construct a subject name using record name strategy and
// pass the result to a user-supplied function, where, for example, the subject might be pushed
// out to schema registry. If the user supplied function returns an error, subsequent call to
// Serde.Encode will short circuit, returning that error.
func ValueSubjectRecordName[T any](record_name string, fn func(string) error) SerdeOpt[T] {
	return valueSubject[T](record_name, fn)
}

// ValueSubjectTopicRecordName tells Serde to construct a subject name using topic record name
// strategy and pass the result to a user-supplied function, where, for example, the subject
// might be pushed out to schema registry. If the user supplied function returns an error,
// subsequent call to Serde.Encode will short circuit, returning that error.
func ValueSubjectTopicRecordName[T any](topic string, record_name string, fn func(string) error) SerdeOpt[T] {
	return valueSubject[T](fmt.Sprintf("%s-%s", topic, record_name), fn)
}

type idSerde[T any] struct {
	encode       func(T) ([]byte, error)
	appendEncode func([]byte, T) ([]byte, error)
	decode       func([]byte, T) error
	ks_err       error
	vs_err       error
}

func (s *idSerde[T]) isEncoder() bool {
	return s.encode != nil || s.appendEncode != nil
}

// Serde encodes and decodes values according to the schema registry wire
// format. A Serde itself does not perform schema auto-discovery and type
// auto-decoding. To aid in strong typing and validated encoding/decoding, you
// must register IDs and values to how to encode or decode them.
//
// To use a Serde for encoding, you must pre-register schema ids and values you
// will encode, and then you can use the encode functions. The latest registered
// ID that supports encoding will be used to encode.
//
// To use a Serde for decoding, you can either pre-register schema ids and
// values you will consume, or you can discover the schema every time you
// receive an ErrNotRegistered error from decode.
type Serde[T any] struct {
	ids             map[int]idSerde[T]
	encodingVersion int

	defaults []SerdeOpt[T]
}

// SetDefaults sets default options to apply to every registered type. These
// options are always applied first, so you can override them as necessary when
// registering.
//
// This can be useful if you always want to use the same encoding or decoding
// functions.
func (s *Serde[T]) SetDefaults(opts ...SerdeOpt[T]) {
	s.defaults = opts
}

// Register registers a schema ID and the value it corresponds to, as well as
// the encoding or decoding functions. You need to register functions depending
// on whether you are only encoding, only decoding, or both.
func (s *Serde[T]) Register(id int, opts ...SerdeOpt[T]) {
	var i idSerde[T]
	for _, o := range s.defaults {
		o.apply(&i)
	}
	for _, o := range opts {
		o.apply(&i)
	}
	if s.ids == nil {
		s.ids = make(map[int]idSerde[T])
	}
	s.ids[id] = i

	if i.isEncoder() && id > s.encodingVersion {
		s.encodingVersion = id
	} else if !i.isEncoder() && id == s.encodingVersion {
		// Someone unregistered an encoder, we need to go back
		// and find the latest
		max := 0
		for id, other := range s.ids {
			if other.isEncoder() && id > max {
				max = id
			}
		}
		s.encodingVersion = max
	}
}

// Encode encodes a value according to the schema registry wire format and
// returns it. If EncodeFn was not used, this returns ErrNotRegistered.
func (s *Serde[T]) Encode(v T) ([]byte, error) {
	return s.AppendEncode(nil, v)
}

// AppendEncode appends an encoded value to b according to the schema registry
// wire format and returns it. If EncodeFn was not used, this returns
// ErrNotRegistered.
func (s *Serde[T]) AppendEncode(b []byte, v T) ([]byte, error) {
	if s.ids == nil {
		return b, ErrNotRegistered
	}
	idserde, ok := s.ids[s.encodingVersion]
	if !ok || !idserde.isEncoder() {
		return b, ErrNotRegistered
	}

	if idserde.ks_err != nil {
		return b, idserde.ks_err
	} else if idserde.vs_err != nil {
		return b, idserde.vs_err
	}

	// write the magic leading byte, then the id in big endian
	b = append(b, 0, 0, 0, 0, 0)
	binary.BigEndian.PutUint32(b[1:5], uint32(s.encodingVersion))

	if idserde.appendEncode != nil {
		return idserde.appendEncode(b, v)
	}
	encoded, err := idserde.encode(v)
	if err != nil {
		return nil, err
	}
	return append(b, encoded...), nil
}

// MustEncode returns the value of Encode, panicking on error. This is a
// shortcut for if your encode function cannot error.
func (s *Serde[T]) MustEncode(v T) []byte {
	b, err := s.Encode(v)
	if err != nil {
		panic(err)
	}
	return b
}

// MustAppendEncode returns the value of AppendEncode, panicking on error.
// This is a shortcut for if your encode function cannot error.
func (s *Serde[T]) MustAppendEncode(b []byte, v T) []byte {
	b, err := s.AppendEncode(b, v)
	if err != nil {
		panic(err)
	}
	return b
}

// Extract the ID from the header of a schema registry encoded value.
//
// Returns ErrBadHeader if the array is missing the leading magic byte
// or is too small.
func ExtractID(b []byte) (int, error) {
	if b == nil || len(b) < 5 || b[0] != 0 {
		return 0, ErrBadHeader
	}
	return int(binary.BigEndian.Uint32(b[1:5])), nil
}

// Decode decodes b into v. If DecodeFn option was not used, this returns
// ErrNotRegistered.
//
// Serde does not handle references in schemas; it is up to you to register the
// full decode function for any top-level ID, regardless of how many other
// schemas are referenced in top-level ID.
func (s *Serde[T]) Decode(b []byte, v T) error {
	id, err := ExtractID(b)
	if err != nil {
		return err
	}
	if s.ids == nil {
		return ErrNotRegistered
	}
	sr, ok := s.ids[id]
	if !ok || sr.decode == nil {
		return ErrNotRegistered
	}
	return sr.decode(b[5:], v)
}
