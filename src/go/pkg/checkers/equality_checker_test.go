package checkers

import (
	"errors"
	"reflect"
	"testing"
)

func Test_equalityChecker_Check(t *testing.T) {
	type fields struct {
		getCurrent func() (interface{}, error)
		desc       string
		severity   Severity
		required   interface{}
	}
	tests := []struct {
		name   string
		fields fields
		want   *CheckResult
	}{
		{
			name: "Shall return valid result when required == current",
			fields: fields{
				desc:       "Some desc",
				getCurrent: func() (interface{}, error) { return "STR_1", nil },
				required:   "STR_1",
				severity:   Warning,
			},
			want: &CheckResult{
				IsOk:    true,
				Err:     nil,
				Current: "STR_1",
			},
		},
		{
			name: "Shall return valid result when required == current for bool",
			fields: fields{
				desc:       "Some desc",
				getCurrent: func() (interface{}, error) { return true, nil },
				required:   true,
				severity:   Warning,
			},
			want: &CheckResult{
				IsOk:    true,
				Err:     nil,
				Current: "true",
			},
		},
		{
			name: "Shall return not valid result when required != current",
			fields: fields{
				desc:       "Some desc",
				getCurrent: func() (interface{}, error) { return "STR_1", nil },
				required:   "STR_2",
				severity:   Warning,
			},
			want: &CheckResult{
				IsOk:    false,
				Err:     nil,
				Current: "STR_1",
			},
		},
		{
			name: "Shall return result with an error when getCurrent returns an error",
			fields: fields{
				getCurrent: func() (interface{}, error) { return "", errors.New("e") },
				required:   "STR_2",
				severity:   Warning,
			},
			want: &CheckResult{
				IsOk: false,
				Err:  errors.New("e"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := NewEqualityChecker(
				tt.fields.desc,
				tt.fields.severity,
				tt.fields.required,
				tt.fields.getCurrent,
			)
			if got := v.Check(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("equalityChecker.Check() = %v, want %v", got, tt.want)
			}
		})
	}
}
