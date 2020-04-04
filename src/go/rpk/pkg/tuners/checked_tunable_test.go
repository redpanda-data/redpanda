package tuners

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

type checkedTunerMock struct {
	checkCalled bool
	tuneCalled  bool
	supported   func() (bool, string)
	check       func() *CheckResult
	tune        func() TuneResult
}

func (c *checkedTunerMock) Id() CheckerID {
	return 1
}

func (c *checkedTunerMock) GetDesc() string {
	return "mocked check"
}

func (c *checkedTunerMock) Check() *CheckResult {
	c.checkCalled = true
	return c.check()
}

func (c *checkedTunerMock) GetSeverity() Severity {
	return Fatal
}

func (c *checkedTunerMock) GetRequiredAsString() string {
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
			want: NewTuneError(errors.New(
				"System tuning was not succesful. Check" +
					" 'mocked check' failed. Required: 'r', current: 'smth'",
			)),
			expectTuneCalled: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &checkedTunerMock{
				supported: func() (bool, string) {
					return true, ""
				},
				check: tt.check,
				tune:  tt.tune,
			}
			ct := NewCheckedTunable(c, c.Tune, c.CheckIfSupported, false)
			got := ct.Tune()
			require.Equal(t, tt.expectTuneCalled, c.tuneCalled)
			require.Equal(t, tt.want, got)
		})
	}
}
