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

func Test_kernelVersionChecker_Check(t *testing.T) {
	tests := []struct {
		name           string
		check          func(c int) bool
		renderRequired func() string
		getCurrent     func() (string, error)
		desc           string
		severity       Severity
		want           *CheckResult
	}{
		{
			name:           "Shall return valid result when condition is met",
			check:          func(c int) bool { return c == 26 },
			renderRequired: func() string { return "0" },
			getCurrent:     func() (string, error) { return "3.19.0", nil },
			want: &CheckResult{
				CheckerID: KernelVersion,
				IsOk:      true,
				Current:   "3.19.0",
				Desc:      "Kernel Version",
				Severity:  Warning,
				Required:  "3.19",
			},
		},
		{
			name:           "Shall return valid result when minor is higher",
			check:          func(c int) bool { return c == 26 },
			renderRequired: func() string { return "0" },
			getCurrent:     func() (string, error) { return "3.20.0", nil },
			want: &CheckResult{
				CheckerID: KernelVersion,
				IsOk:      true,
				Current:   "3.20.0",
				Desc:      "Kernel Version",
				Severity:  Warning,
				Required:  "3.19",
			},
		},
		{
			name:           "Shall return valid result when major is higher",
			check:          func(c int) bool { return c == 26 },
			renderRequired: func() string { return "0" },
			getCurrent:     func() (string, error) { return "5.1.0", nil },
			want: &CheckResult{
				CheckerID: KernelVersion,
				IsOk:      true,
				Current:   "5.1.0",
				Desc:      "Kernel Version",
				Severity:  Warning,
				Required:  "3.19",
			},
		},
		{
			name:           "Shall return invalid result when minor is lower",
			check:          func(c int) bool { return c == 26 },
			renderRequired: func() string { return "0" },
			getCurrent:     func() (string, error) { return "3.18.0", nil },
			want: &CheckResult{
				CheckerID: KernelVersion,
				IsOk:      false,
				Current:   "3.18.0",
				Desc:      "Kernel Version",
				Severity:  Warning,
				Required:  "3.19",
				Err:       errors.New("kernel version is too old"),
			},
		},
		{
			name:           "Shall return invalid result when major is lower",
			check:          func(c int) bool { return c == 26 },
			renderRequired: func() string { return "0" },
			getCurrent:     func() (string, error) { return "2.19.0", nil },
			want: &CheckResult{
				CheckerID: KernelVersion,
				IsOk:      false,
				Current:   "2.19.0",
				Desc:      "Kernel Version",
				Severity:  Warning,
				Required:  "3.19",
				Err:       errors.New("kernel version is too old"),
			},
		},
		{
			name:           "Shall return invalid result when patch is missing",
			check:          func(c int) bool { return c == 26 },
			renderRequired: func() string { return "0" },
			getCurrent:     func() (string, error) { return "3.19", nil },
			want: &CheckResult{
				CheckerID: KernelVersion,
				IsOk:      false,
				Current:   "3.19",
				Desc:      "Kernel Version",
				Severity:  Warning,
				Required:  "3.19",
				Err:       errors.New("failed to parse kernel version"),
			},
		},
		{
			name:           "Shall return valid result when data after patch",
			check:          func(c int) bool { return c == 26 },
			renderRequired: func() string { return "0" },
			getCurrent:     func() (string, error) { return "5.8.0-19-generic", nil },
			want: &CheckResult{
				CheckerID: KernelVersion,
				IsOk:      true,
				Current:   "5.8.0-19-generic",
				Desc:      "Kernel Version",
				Severity:  Warning,
				Required:  "3.19",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := NewKernelVersionChecker(tt.getCurrent)
			got := v.Check()
			require.Exactly(t, tt.want, got)
		})
	}
}
