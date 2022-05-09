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
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

type checkedTunerMock struct {
	checkCalled bool
	tuneCalled  bool
	severity    Severity
	supported   func() (bool, string)
	check       func() *CheckResult
	tune        func() TuneResult
}

func (*checkedTunerMock) ID() CheckerID {
	return 1
}

func (*checkedTunerMock) GetDesc() string {
	return "mocked check"
}

func (c *checkedTunerMock) Check() *CheckResult {
	c.checkCalled = true
	return c.check()
}

func (c *checkedTunerMock) GetSeverity() Severity {
	return c.severity
}

func (*checkedTunerMock) GetRequiredAsString() string {
	return "r"
}

func (c *checkedTunerMock) Tune() TuneResult {
	c.tuneCalled = true
	return c.tune()
}

func (c *checkedTunerMock) CheckIfSupported() (bool, string) {
	return c.supported()
}

func TestTune(t *testing.T) {
	tests := []struct {
		name             string
		check            func() *CheckResult
		tune             func() TuneResult
		severity         Severity
		want             TuneResult
		expectTuneCalled bool
	}{
		{
			name: "should not execute tuner when condition is already met",
			check: func() *CheckResult {
				return &CheckResult{
					IsOk: true,
				}
			},
			tune: func() TuneResult {
				return NewTuneResult(false)
			},
			severity:         Fatal,
			want:             NewTuneResult(false),
			expectTuneCalled: false,
		},
		{
			name: "Tune result should contain an error if tuning was not successful",
			check: func() *CheckResult {
				return &CheckResult{
					IsOk:    false,
					Current: "smth",
				}
			},
			tune: func() TuneResult {
				return NewTuneResult(false)
			},
			severity: Fatal,
			want: NewTuneError(errors.New(
				"check 'mocked check' failed after its associated tuners ran. Severity: Fatal, required value: 'r', current value: 'smth'",
			)),
			expectTuneCalled: true,
		},
		{
			name: "should not fail if the checker fails and its severity is Warning",
			check: func() *CheckResult {
				return &CheckResult{
					IsOk:    false,
					Current: "smth",
				}
			},
			tune: func() TuneResult {
				return NewTuneResult(false)
			},
			severity:         Warning,
			want:             NewTuneResult(false),
			expectTuneCalled: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &checkedTunerMock{
				supported: func() (bool, string) {
					return true, ""
				},
				check:    tt.check,
				tune:     tt.tune,
				severity: tt.severity,
			}
			ct := NewCheckedTunable(c, c.Tune, c.CheckIfSupported, false)
			got := ct.Tune()
			require.Equal(t, tt.expectTuneCalled, c.tuneCalled)
			require.Equal(t, tt.want, got)
		})
	}
}
