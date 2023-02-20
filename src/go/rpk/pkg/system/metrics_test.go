// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package system_test

import (
	"testing"
	"time"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/system"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

func TestGatherMetrics(t *testing.T) {
	tests := []struct {
		name           string
		before         func(fs afero.Fs) error
		conf           func() *config.Config
		expectedErrMsg string
	}{{
		name: "it should fail if the process's stat file doesn't exist",
		before: func(fs afero.Fs) error {
			return afero.WriteFile(
				fs,
				config.DevDefault().PIDFile(),
				// Usual /proc/sys/kernel/pid_max value
				[]byte("4194304"),
				0o755,
			)
		},
		expectedErrMsg: "/proc/4194304/stat",
	}, {
		name:           "it should fail if the PID file doesn't exist",
		expectedErrMsg: "the local redpanda process isn't running",
	}, {
		name: "it should fail if the CPU utime can't be parsed",
		before: func(fs afero.Fs) error {
			err := afero.WriteFile(
				fs,
				config.DevDefault().PIDFile(),
				// Usual /proc/sys/kernel/pid_max value
				[]byte("4194304"),
				0o755,
			)
			if err != nil {
				return err
			}
			content := "4194304 (redpanda) S 1 2680757 2680757 0 -1 4194304 18612 8456 1 0 invalid-utime 422288 10 11 20 0 32 0 88937938 17593454493696 811178 18446744073709551615 1 1 0 0 0 0 2143394384 0 17442 0 0 0 17 1 0 0 0 0 0 0 0 0 0 0 0 0 0"
			return afero.WriteFile(
				fs,
				"/proc/4194304/stat",
				[]byte(content),
				0o755,
			)
		},
		expectedErrMsg: `parsing "invalid-utime": invalid syntax`,
	}, {
		name: "it should fail if the CPU stime can't be parsed",
		before: func(fs afero.Fs) error {
			err := afero.WriteFile(
				fs,
				config.DevDefault().PIDFile(),
				// Usual /proc/sys/kernel/pid_max value
				[]byte("4194304"),
				0o755,
			)
			if err != nil {
				return err
			}
			content := "4194304 (redpanda) S 1 2680757 2680757 0 -1 4194304 18612 8456 1 0 398600 invalid-stime 10 11 20 0 32 0 88937938 17593454493696 811178 18446744073709551615 1 1 0 0 0 0 2143394384 0 17442 0 0 0 17 1 0 0 0 0 0 0 0 0 0 0 0 0 0"
			return afero.WriteFile(
				fs,
				"/proc/4194304/stat",
				[]byte(content),
				0o755,
			)
		},
		expectedErrMsg: `parsing "invalid-stime": invalid syntax`,
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(st *testing.T) {
			fs := afero.NewMemMapFs()
			conf := config.DevDefault()
			if tt.conf != nil {
				conf = tt.conf()
			}
			if tt.before != nil {
				require.NoError(st, tt.before(fs))
			}
			_, err := system.GatherMetrics(fs, 50*time.Millisecond, *conf)
			if tt.expectedErrMsg != "" {
				require.Error(st, err)
				require.Contains(st, err.Error(), tt.expectedErrMsg)
				return
			}
		})
	}
}
