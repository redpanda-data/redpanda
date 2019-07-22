package tuners

import (
	"errors"
	"reflect"
	"testing"
	"vectorized/checkers"
)

type checkerMock struct {
	checkers.Checker
	checkFunction func() *checkers.CheckResult
}

func (c *checkerMock) GetDesc() string {
	return "mocked checker"
}

func (c *checkerMock) Check() *checkers.CheckResult {
	return c.checkFunction()
}

func (c *checkerMock) GetRequiredAsString() string {
	return "r"
}

func Test_checkTunable_Tune(t *testing.T) {
	type fields struct {
		checker         checkers.Checker
		tuneAction      func() TuneResult
		supportedAction func() (supported bool, reason string)
	}
	var tuneCallsCnt int
	var checkCallsCnt int
	tests := []struct {
		name              string
		tuneCallsCounter  int
		fields            fields
		want              TuneResult
		numberOfTuneCalls int
	}{
		{
			name: "should not execute tuner when condition is already met",
			fields: fields{
				checker: &checkerMock{
					checkFunction: func() *checkers.CheckResult {
						return &checkers.CheckResult{
							IsOk: true,
						}
					},
				},
				tuneAction: func() TuneResult {
					tuneCallsCnt++
					return NewTuneResult(false)
				},
				supportedAction: func() (supported bool, reason string) {
					return true, ""
				},
			},
			want:              NewTuneResult(false),
			numberOfTuneCalls: 0,
		},
		{
			name: "should not execute tuner when condition is already met",
			fields: fields{
				checker: &checkerMock{
					checkFunction: func() *checkers.CheckResult {
						res := &checkers.CheckResult{
							IsOk: checkCallsCnt > 0,
						}
						checkCallsCnt++
						return res
					},
				},
				tuneAction: func() TuneResult {
					tuneCallsCnt++
					return NewTuneResult(false)
				},
				supportedAction: func() (supported bool, reason string) {
					return true, ""
				},
			},
			want:              NewTuneResult(false),
			numberOfTuneCalls: 1,
		},
		{
			name: "Tune result should contain an error if tuning was not successfull",
			fields: fields{
				checker: &checkerMock{
					checkFunction: func() *checkers.CheckResult {
						res := &checkers.CheckResult{
							IsOk:    false,
							Current: "smth",
						}
						checkCallsCnt++
						return res
					},
				},
				tuneAction: func() TuneResult {
					tuneCallsCnt++
					return NewTuneResult(false)
				},
				supportedAction: func() (supported bool, reason string) {
					return true, ""
				},
			},
			want: NewTuneError(errors.New("System tuning was not succesfull, " +
				"check 'mocked checker' failed, required: 'r', current 'smth'")),
			numberOfTuneCalls: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tunable := &checkedTunable{
				checker:         tt.fields.checker,
				tuneAction:      tt.fields.tuneAction,
				supportedAction: tt.fields.supportedAction,
			}
			if got := tunable.Tune(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("checkTunable.Tune() = %v, want %v", got, tt.want)
			}
			if tuneCallsCnt != tt.numberOfTuneCalls {
				t.Errorf("checkTunable.Tune() calls = %v, want %v",
					tuneCallsCnt, tt.numberOfTuneCalls)
			}
			tuneCallsCnt = 0
			checkCallsCnt = 0
		})
	}
}
