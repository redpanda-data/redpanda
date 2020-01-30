package tuners

import (
	"fmt"
	"reflect"
	"testing"
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
			name: "shall return success when all tune opertions are suceessful",
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
			tunable := &aggregatedTunable{
				tunables: tt.fields.tunables,
			}
			if got := tunable.Tune(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("aggregatedTunable.Tune() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_aggregatedTunable_CheckIfSupported(t *testing.T) {
	type fields struct {
		tunables []Tunable
	}
	tests := []struct {
		name          string
		fields        fields
		wantSupported bool
		wantReason    string
	}{
		{
			name: "shall be supported when all tunables are",
			fields: fields{
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
			},
			wantSupported: true,
			wantReason:    "",
		},
		{
			name: "shall forward reson when one of tunables is not supported",
			fields: fields{
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
			},
			wantSupported: false,
			wantReason:    "Why not",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tunable := &aggregatedTunable{
				tunables: tt.fields.tunables,
			}
			gotSupported, gotReason := tunable.CheckIfSupported()
			if gotSupported != tt.wantSupported {
				t.Errorf("aggregatedTunable.CheckIfSupported() gotSupported = %v, want %v", gotSupported, tt.wantSupported)
			}
			if gotReason != tt.wantReason {
				t.Errorf("aggregatedTunable.CheckIfSupported() gotReason = %v, want %v", gotReason, tt.wantReason)
			}
		})
	}
}
