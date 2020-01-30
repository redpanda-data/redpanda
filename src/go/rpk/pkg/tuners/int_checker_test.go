package tuners

import (
	"errors"
	"reflect"
	"testing"
)

func Test_intChecker_Check(t *testing.T) {
	type fields struct {
		check          func(c int) bool
		renderRequired func() string
		getCurrent     func() (int, error)
		desc           string
		severity       Severity
	}
	tests := []struct {
		name   string
		fields fields
		want   *CheckResult
	}{
		{
			name: "Shall return valid result when condition is met",
			fields: fields{
				check:          func(c int) bool { return c == 0 },
				renderRequired: func() string { return "0" },
				desc:           "Some desc",
				getCurrent:     func() (int, error) { return 0, nil },
				severity:       Warning,
			},
			want: &CheckResult{
				IsOk:     true,
				Current:  "0",
				Desc:     "Some desc",
				Severity: Warning,
				Required: "0",
			},
		},
		{
			name: "Shall return not valid result when condition is not met",
			fields: fields{
				check:          func(c int) bool { return c == 0 },
				renderRequired: func() string { return "0" },
				desc:           "Some desc",
				getCurrent:     func() (int, error) { return 1, nil },
				severity:       Warning,
			},
			want: &CheckResult{
				IsOk:     false,
				Current:  "1",
				Desc:     "Some desc",
				Severity: Warning,
				Required: "0",
			},
		},
		{
			name: "Shall return result with an error when getCurretn returns an error",
			fields: fields{
				check:          func(c int) bool { return c == 0 },
				renderRequired: func() string { return "0" },
				getCurrent:     func() (int, error) { return 0, errors.New("err") },
				severity:       Warning,
			},
			want: &CheckResult{
				IsOk:     false,
				Err:      errors.New("err"),
				Severity: Warning,
				Required: "0",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &intChecker{
				check:          tt.fields.check,
				renderRequired: tt.fields.renderRequired,
				getCurrent:     tt.fields.getCurrent,
				desc:           tt.fields.desc,
				severity:       tt.fields.severity,
			}
			if got := v.Check(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("intChecker.Check() = %v, want %v", got, tt.want)
			}
		})
	}
}
