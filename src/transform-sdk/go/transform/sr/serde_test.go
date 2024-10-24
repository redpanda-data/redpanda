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
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"
)

type oldExample struct {
	A string
	B int
}
type example struct {
	A string
	B int
	C bool
}
type newExample struct {
	A string
	B int
	C bool
	D []string
}

var s Serde[*example]

func init() {
	// Support reading old versions
	s.Register(
		1,
		DecodeFn[*example](func(b []byte, e *example) error {
			return json.Unmarshal(b, e)
		}),
	)
	// The latest version we want to read/write
	s.Register(
		2,
		EncodeFn[*example](func(e *example) ([]byte, error) {
			return json.Marshal(e)
		}),
		DecodeFn[*example](func(b []byte, e *example) error {
			return json.Unmarshal(b, e)
		}),
	)
}

func TestSerdeRoundTrip(t *testing.T) {
	e1 := example{
		A: "foo",
		B: 42,
		C: true,
	}
	b := s.MustEncode(&e1)
	e2 := example{}
	if err := s.Decode(b, &e2); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(e1, e2) {
		t.Error(e1, "!=", e2)
	}
	eOld := oldExample{A: "bar", B: 9}
	b, err := json.Marshal(&eOld)
	if err != nil {
		t.Fatal(err)
	}
	e3 := example{}
	b = append([]byte{0, 0, 0, 0, 1}, b...)
	if err := s.Decode(b, &e3); err != nil {
		t.Fatal(err)
	}
	e4 := example{
		A: "bar",
		B: 9,
		C: false,
	}
	if !reflect.DeepEqual(e3, e4) {
		t.Error(e2, "!=", e4)
	}
}

func TestUnregistered(t *testing.T) {
	e := newExample{
		A: "qux",
		B: 14,
		C: true,
		D: []string{"a", "b", "c"},
	}
	b, err := json.Marshal(&e)
	if err != nil {
		t.Fatal(err)
	}
	b = append([]byte{0, 0, 0, 0, 3}, b...)
	err = s.Decode(b, &example{})
	if err != ErrNotRegistered {
		t.Fatal("unexpected error", err)
	}
}

func TestSubjectNameDerivation(t *testing.T) {
	e1 := example{
		A: "foo",
		B: 42,
		C: true,
	}
	// Register again, with the SNS options
	ks := ""
	vs := ""
	topicName := "demo-topic"
	recordName := "example"

	s.Register(
		3,
		EncodeFn[*example](func(e *example) ([]byte, error) {
			return json.Marshal(e)
		}),
		DecodeFn[*example](func(b []byte, e *example) error {
			return json.Unmarshal(b, e)
		}),
		KeySubjectTopicName[*example](topicName, func(subj_name string) error {
			ks = subj_name
			return nil
		}),
		ValueSubjectRecordName[*example](recordName, func(subj_name string) error {
			vs = subj_name
			return nil
		}),
	)

	if ks != fmt.Sprintf("%s-key", topicName) {
		t.Fatal("Incorrect key subject name TopicNameStrategy", ks)
	}

	if vs != fmt.Sprintf("%s-value", recordName) {
		t.Fatal("Incorrect value subject name for RecordNameStrategy", vs)
	}

	b := s.MustEncode(&e1)
	e2 := example{}
	if err := s.Decode(b, &e2); err != nil {
		t.Fatal(err)
	}

	s.Register(
		4,
		EncodeFn[*example](func(e *example) ([]byte, error) {
			return json.Marshal(e)
		}),
		DecodeFn[*example](func(b []byte, e *example) error {
			return json.Unmarshal(b, e)
		}),
		KeySubjectTopicRecordName[*example](topicName, recordName, func(subj_name string) error {
			ks = subj_name
			return nil
		}),
	)

	if ks != fmt.Sprintf("%s-%s-key", topicName, recordName) {
		t.Fatal("Incorrect key subject name for TopicRecordNameStrategy", ks)
	}

	b = s.MustEncode(&e1)
	e2 = example{}
	if err := s.Decode(b, &e2); err != nil {
		t.Fatal(err)
	}

}

func TestSubjectNameDerivationErrors(t *testing.T) {
	e1 := example{
		A: "foo",
		B: 42,
		C: true,
	}

	var (
		SomeError = errors.New("Oops")
	)

	// Register again, with the SNS options
	topicName := "demo-topic"

	s.Register(
		5,
		EncodeFn[*example](func(e *example) ([]byte, error) {
			return json.Marshal(e)
		}),
		DecodeFn[*example](func(b []byte, e *example) error {
			return json.Unmarshal(b, e)
		}),
		KeySubjectTopicName[*example](topicName, func(subj_name string) error {
			return SomeError
		}),
	)

	// Check that encoding fails and reports the error from the user-supplied function
	_, err := s.Encode(&e1)
	if err != SomeError {
		t.Fatal("unexpected error", err)
	}

	s.Register(
		6,
		EncodeFn[*example](func(e *example) ([]byte, error) {
			return json.Marshal(e)
		}),
		DecodeFn[*example](func(b []byte, e *example) error {
			return json.Unmarshal(b, e)
		}),
		ValueSubjectTopicName[*example](topicName, func(subj_name string) error {
			return SomeError
		}),
	)

	// Check that encoding fails and reports the error from the user-supplied function
	_, err = s.Encode(&e1)
	if err != SomeError {
		t.Fatal("unexpected error", err)
	}
}
