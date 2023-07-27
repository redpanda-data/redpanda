// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package tuners

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

type mockedTunable struct {
	tune             func() TuneResult
	checkIfSupported func() (bool, string)
}

func (t *mockedTunable) CheckIfSupported() (supported bool, reason string) {
	return t.checkIfSupported()
}

func (t *mockedTunable) Tune() TuneResult {
	return t.tune()
}

func Test_aggregatedTunable_Tune(t *testing.T) {
	type fields struct {
		Tunable  Tunable
		tunables []Tunable
	}
	tests := []struct {
		name   string
		fields fields
		want   TuneResult
	}{
		{
			name: "shall return success when all tune operations are successful",
			fields: fields{
				tunables: []Tunable{
					&mockedTunable{
						tune: func() TuneResult {
							return NewTuneResult(false)
						},
					},
					&mockedTunable{
						tune: func() TuneResult {
							return NewTuneResult(false)
						},
					},
					&mockedTunable{
						tune: func() TuneResult {
							return NewTuneResult(false)
						},
					},
				},
			},
			want: NewTuneResult(false),
		},
		{
			name: "shall return error when at least one of result contains an error",
			fields: fields{
				tunables: []Tunable{
					&mockedTunable{
						tune: func() TuneResult {
							return NewTuneResult(false)
						},
					},
					&mockedTunable{
						tune: func() TuneResult {
							return NewTuneError(fmt.Errorf("Test error"))
						},
					},
					&mockedTunable{
						tune: func() TuneResult {
							return NewTuneResult(false)
						},
					},
				},
			},
			want: NewTuneError(fmt.Errorf("Test error")),
		},
		{
			name: "shall request for reboot if at least one of result requests it",
			fields: fields{
				tunables: []Tunable{
					&mockedTunable{
						tune: func() TuneResult {
							return NewTuneResult(false)
						},
					},
					&mockedTunable{
						tune: func() TuneResult {
							return NewTuneResult(true)
						},
					},
					&mockedTunable{
						tune: func() TuneResult {
							return NewTuneResult(false)
						},
					},
				},
			},
			want: NewTuneResult(true),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tunable := &aggregatedTunable{tt.fields.tunables}
			got := tunable.Tune()
			require.Exactly(t, tt.want, got)
		})
	}
}

func Test_aggregatedTunable_CheckIfSupported(t *testing.T) {
	tests := []struct {
		name          string
		tunables      []Tunable
		wantSupported bool
		wantReason    string
	}{
		{
			name: "shall be supported when all tunables are",
			tunables: []Tunable{
				&mockedTunable{
					checkIfSupported: func() (bool, string) {
						return true, ""
					},
				},
				&mockedTunable{
					checkIfSupported: func() (bool, string) {
						return true, ""
					},
				},
				&mockedTunable{
					checkIfSupported: func() (bool, string) {
						return true, ""
					},
				},
			},
			wantSupported: true,
			wantReason:    "",
		},
		{
			name: "shall forward reason when one of tunables is not supported",
			tunables: []Tunable{
				&mockedTunable{
					checkIfSupported: func() (bool, string) {
						return true, ""
					},
				},
				&mockedTunable{
					checkIfSupported: func() (bool, string) {
						return false, "Why not"
					},
				},
				&mockedTunable{
					checkIfSupported: func() (bool, string) {
						return true, ""
					},
				},
			},
			wantSupported: false,
			wantReason:    "Why not",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tunable := &aggregatedTunable{tt.tunables}
			gotSupported, gotReason := tunable.CheckIfSupported()
			require.Equal(t, tt.wantSupported, gotSupported)
			require.Equal(t, tt.wantReason, gotReason)
		})
	}
}
