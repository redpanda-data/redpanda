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

func Test_intChecker_Check(t *testing.T) {
	tests := []struct {
		name           string
		check          func(c int) bool
		renderRequired func() string
		getCurrent     func() (int, error)
		desc           string
		severity       Severity
		want           *CheckResult
	}{
		{
			name:           "Shall return valid result when condition is met",
			check:          func(c int) bool { return c == 0 },
			renderRequired: func() string { return "0" },
			getCurrent:     func() (int, error) { return 0, nil },
			want: &CheckResult{
				IsOk:     true,
				Current:  "0",
				Desc:     "An int check",
				Severity: Warning,
				Required: "0",
			},
		},
		{
			name:           "Shall return not valid result when condition is not met",
			check:          func(c int) bool { return c == 0 },
			renderRequired: func() string { return "0" },
			getCurrent:     func() (int, error) { return 1, nil },
			want: &CheckResult{
				IsOk:     false,
				Current:  "1",
				Desc:     "An int check",
				Severity: Warning,
				Required: "0",
			},
		},
		{
			name:           "Shall return result with an error when getCurrent returns an error",
			check:          func(c int) bool { return c == 0 },
			renderRequired: func() string { return "0" },
			getCurrent:     func() (int, error) { return 0, errors.New("err") },
			want: &CheckResult{
				IsOk:     false,
				Desc:     "An int check",
				Err:      errors.New("err"),
				Severity: Warning,
				Required: "0",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := NewIntChecker(
				0,
				"An int check",
				Warning,
				tt.check,
				tt.renderRequired,
				tt.getCurrent,
			)
			got := v.Check()
			require.Exactly(t, tt.want, got)
		})
	}
}
