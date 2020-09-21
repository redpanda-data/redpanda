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
			getCurrent:     func() (string, error) { return "4.19.0", nil },
			want: &CheckResult{
				CheckerId: KernelVersion,
				IsOk:      true,
				Current:   "4.19.0",
				Desc:      "Kernel Version",
				Severity:  Warning,
				Required:  "4.19",
			},
		},
		{
			name:           "Shall return valid result when minor is higher",
			check:          func(c int) bool { return c == 26 },
			renderRequired: func() string { return "0" },
			getCurrent:     func() (string, error) { return "4.20.0", nil },
			want: &CheckResult{
				CheckerId: KernelVersion,
				IsOk:      true,
				Current:   "4.20.0",
				Desc:      "Kernel Version",
				Severity:  Warning,
				Required:  "4.19",
			},
		},
		{
			name:           "Shall return valid result when major is higher",
			check:          func(c int) bool { return c == 26 },
			renderRequired: func() string { return "0" },
			getCurrent:     func() (string, error) { return "5.1.0", nil },
			want: &CheckResult{
				CheckerId: KernelVersion,
				IsOk:      true,
				Current:   "5.1.0",
				Desc:      "Kernel Version",
				Severity:  Warning,
				Required:  "4.19",
			},
		},
		{
			name:           "Shall return invalid result when minor is lower",
			check:          func(c int) bool { return c == 26 },
			renderRequired: func() string { return "0" },
			getCurrent:     func() (string, error) { return "4.18.0", nil },
			want: &CheckResult{
				CheckerId: KernelVersion,
				IsOk:      false,
				Current:   "4.18.0",
				Desc:      "Kernel Version",
				Severity:  Warning,
				Required:  "4.19",
				Err:       errors.New("kernel version is too old"),
			},
		},
		{
			name:           "Shall return invalid result when major is lower",
			check:          func(c int) bool { return c == 26 },
			renderRequired: func() string { return "0" },
			getCurrent:     func() (string, error) { return "3.19.0", nil },
			want: &CheckResult{
				CheckerId: KernelVersion,
				IsOk:      false,
				Current:   "3.19.0",
				Desc:      "Kernel Version",
				Severity:  Warning,
				Required:  "4.19",
				Err:       errors.New("kernel version is too old"),
			},
		},
		{
			name:           "Shall return invalid result when patch is missing",
			check:          func(c int) bool { return c == 26 },
			renderRequired: func() string { return "0" },
			getCurrent:     func() (string, error) { return "4.19", nil },
			want: &CheckResult{
				CheckerId: KernelVersion,
				IsOk:      false,
				Current:   "4.19",
				Desc:      "Kernel Version",
				Severity:  Warning,
				Required:  "4.19",
				Err:       errors.New("failed to parse kernel version"),
			},
		},
		{
			name:           "Shall return valid result when data after patch",
			check:          func(c int) bool { return c == 26 },
			renderRequired: func() string { return "0" },
			getCurrent:     func() (string, error) { return "5.8.0-19-generic", nil },
			want: &CheckResult{
				CheckerId: KernelVersion,
				IsOk:      true,
				Current:   "5.8.0-19-generic",
				Desc:      "Kernel Version",
				Severity:  Warning,
				Required:  "4.19",
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
