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

func Test_equalityChecker_Check(t *testing.T) {
	tests := []struct {
		name       string
		getCurrent func() (interface{}, error)
		desc       string
		severity   Severity
		required   interface{}
		want       *CheckResult
	}{
		{
			name:       "Shall return valid result when required == current",
			desc:       "Some desc",
			getCurrent: func() (interface{}, error) { return "STR_1", nil },
			required:   "STR_1",
			severity:   Warning,
			want: &CheckResult{
				IsOk:     true,
				Current:  "STR_1",
				Required: "STR_1",
				Severity: Warning,
				Desc:     "Some desc",
			},
		},
		{
			name:       "Shall return valid result when required == current for bool",
			desc:       "Some desc",
			getCurrent: func() (interface{}, error) { return true, nil },
			required:   true,
			severity:   Warning,
			want: &CheckResult{
				IsOk:     true,
				Current:  "true",
				Required: "true",
				Severity: Warning,
				Desc:     "Some desc",
			},
		},
		{
			name:       "Shall return not valid result when required != current",
			desc:       "Some desc",
			getCurrent: func() (interface{}, error) { return "STR_1", nil },
			required:   "STR_2",
			severity:   Warning,
			want: &CheckResult{
				IsOk:     false,
				Current:  "STR_1",
				Required: "STR_2",
				Severity: Warning,
				Desc:     "Some desc",
			},
		},
		{
			name:       "Shall return result with an error when getCurrent returns an error",
			getCurrent: func() (interface{}, error) { return "", errors.New("e") },
			required:   "STR_2",
			severity:   Warning,
			want: &CheckResult{
				IsOk:     false,
				Err:      errors.New("e"),
				Required: "STR_2",
				Severity: Warning,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := NewEqualityChecker(
				0,
				tt.desc,
				tt.severity,
				tt.required,
				tt.getCurrent,
			)
			got := v.Check()
			require.Exactly(t, tt.want, got)
		})
	}
}
