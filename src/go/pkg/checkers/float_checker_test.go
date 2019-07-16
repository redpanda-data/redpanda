package checkers

import (
	"errors"
	"reflect"
	"testing"
)

func Test_floatChecker_Check(t *testing.T) {
	type fields struct {
		check          func(c float64) bool
		renderRequired func() string
		getCurrent     func() (float64, error)
		desc           string
		isCritical     bool
	}
	tests := []struct {
		name   string
		fields fields
		want   *CheckResult
	}{
		{
			name: "Shall return valid result when condition is met",
			fields: fields{
				check:          func(c float64) bool { return c >= 0.0 },
				renderRequired: func() string { return ">= 0.0" },
				desc:           "Some desc",
				getCurrent:     func() (float64, error) { return 0.0, nil },
				isCritical:     false,
			},
			want: &CheckResult{
				IsOk:    true,
				Err:     nil,
				Current: "0.00",
			},
		},
		{
			name: "Shall return not valid result when condition is not met",
			fields: fields{
				check:          func(c float64) bool { return c == 0.1 },
				renderRequired: func() string { return "0.1" },
				desc:           "Some desc",
				getCurrent:     func() (float64, error) { return 1.1, nil },
				isCritical:     false,
			},
			want: &CheckResult{
				IsOk:    false,
				Err:     nil,
				Current: "1.10",
			},
		},
		{
			name: "Shall return result with an error when getCurretn returns an error",
			fields: fields{
				check:          func(c float64) bool { return c < 10.0 },
				renderRequired: func() string { return "< 10" },
				getCurrent:     func() (float64, error) { return 0.0, errors.New("err") },
				isCritical:     false,
			},
			want: &CheckResult{
				IsOk: false,
				Err:  errors.New("err"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &floatChecker{
				check:          tt.fields.check,
				renderRequired: tt.fields.renderRequired,
				getCurrent:     tt.fields.getCurrent,
				desc:           tt.fields.desc,
				isCritical:     tt.fields.isCritical,
			}
			if got := v.Check(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("floatChecker.Check() = %v, want %v", got, tt.want)
			}
		})
	}
}
