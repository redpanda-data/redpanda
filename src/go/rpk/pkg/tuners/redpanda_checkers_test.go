// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

//go:build linux
// +build linux

package tuners_test

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

func setUpCgroup(fs afero.Fs, file, val string, v2 bool) error {
	if file == "" {
		return nil
	}
	fullPath := "/sys/fs/cgroup" + file
	dir := filepath.Dir(fullPath)
	if err := fs.MkdirAll("/proc/self/", 0o755); err != nil {
		return err
	}
	var contents string
	if v2 {
		cgroup := filepath.Dir(file)
		contents = "0::" + cgroup
	} else {
		contents = `2:cpu,cpuacct:/user.slice
1:name=systemd:/user.slice/user-1.slice/user@1.service
0::/user.slice/user-1000.slice/user@1000.service/gnome-terminal-server.service
`
	}
	err := afero.WriteFile(
		fs,
		"/proc/self/cgroup",
		[]byte(contents),
		0o644,
	)
	if err != nil {
		return err
	}
	if err = fs.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	return afero.WriteFile(fs, fullPath, []byte(val), 0o644)
}

func TestNtpCheckTimeout(t *testing.T) {
	timeout := time.Duration(0)
	check := tuners.NewNTPSyncChecker(timeout, afero.NewMemMapFs())
	res := check.Check()
	require.False(t, res.IsOk, "the NTP check shouldn't have succeeded")
	require.Error(t, res.Err, "the NTP check should have failed with an error")
}

func TestFreeMemoryChecker(t *testing.T) {
	tests := []struct {
		name          string
		memoryLimit   string
		effectiveCpus string
		expectOk      bool
		expectedErr   string
		cgroupsV2     bool
	}{
		{
			name:          "it should pass if the mem. limit per cpu >= 2GB",
			memoryLimit:   "8192000000000",
			effectiveCpus: "4",
			expectOk:      true,
		},
		{
			name:          "it should fail if the mem. limit per cpu < 2GB",
			memoryLimit:   "8192",
			effectiveCpus: "8",
		},
		{
			name:          "it should return an err if the mem. limit value is empty",
			memoryLimit:   "",
			effectiveCpus: "8",
			expectedErr:   "unable to determine cgroup memory limit",
		},
		{
			name:          "it should return an err if the eff. cpus value is empty",
			memoryLimit:   "9800000000",
			effectiveCpus: "",
			expectedErr:   "no value found in /sys/fs/cgroup/cpuset/cpuset.effective_cpus",
		},
		{
			name:          "it should pass if the mem. limit per cpu >= 2GB",
			memoryLimit:   "8192000000000",
			effectiveCpus: "4",
			expectOk:      true,
			cgroupsV2:     true,
		},
		{
			name:          "it should fail if the mem. limit per cpu < 2GB",
			memoryLimit:   "8192",
			effectiveCpus: "8",
			cgroupsV2:     true,
		},
		{
			name:          "it should return an err if the mem. limit value is empty",
			memoryLimit:   "",
			effectiveCpus: "8",
			expectedErr:   "unable to determine cgroup memory limit",
			cgroupsV2:     true,
		},
		{
			name:          "it should return an err if the eff. cpus value is empty",
			memoryLimit:   "9800000000",
			effectiveCpus: "",
			expectedErr:   "no value found in /sys/fs/cgroup/redpanda.slice/redpanda.service/cpuset.cpus.effective",
			cgroupsV2:     true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := afero.NewMemMapFs()
			if tt.cgroupsV2 {
				err := setUpCgroup(
					fs,
					"/redpanda.slice/redpanda.service/memory.max",
					tt.memoryLimit,
					tt.cgroupsV2,
				)
				require.NoError(t, err)
				err = setUpCgroup(
					fs,
					"/redpanda.slice/redpanda.service/cpuset.cpus.effective",
					tt.effectiveCpus,
					tt.cgroupsV2,
				)
				require.NoError(t, err)
			} else {
				err := setUpCgroup(
					fs,
					"/memory/memory.limit_in_bytes",
					tt.memoryLimit,
					tt.cgroupsV2,
				)
				require.NoError(t, err)
				err = setUpCgroup(
					fs,
					"/cpuset/cpuset.effective_cpus",
					tt.effectiveCpus,
					tt.cgroupsV2,
				)
				require.NoError(t, err)
			}
			c := tuners.NewMemoryChecker(fs)
			res := c.Check()
			if tt.expectedErr != "" {
				require.EqualError(t, res.Err, tt.expectedErr)
				return
			}
			require.NoError(t, res.Err, tt.name)
			require.Equal(t, tt.expectOk, res.IsOk)
		})
	}
}
