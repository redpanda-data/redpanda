// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package system_test

import (
	"math"
	"path/filepath"
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/system"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
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

func TestReadCgroupProp(t *testing.T) {
	tests := []struct {
		name        string
		value       string
		expected    uint64
		file        string
		f           func(afero.Fs) (uint64, error)
		expectedErr string
		cgroupsV2   bool
	}{
		{
			name:     "it should read the mem limit (v1)",
			value:    "12345687",
			expected: uint64(12345687),
			file:     "/memory/memory.limit_in_bytes",
			f:        system.ReadCgroupMemLimitBytes,
		},
		{
			name:     "it should read the mem limit ('max') (v1)",
			value:    "max",
			expected: math.MaxUint64,
			file:     "/memory/memory.limit_in_bytes",
			f:        system.ReadCgroupMemLimitBytes,
		},
		{
			name:        "it should fail if the mem file is empty (v1)",
			value:       "",
			file:        "/memory/memory.limit_in_bytes",
			f:           system.ReadCgroupMemLimitBytes,
			expectedErr: "no value found in /sys/fs/cgroup/memory/memory.limit_in_bytes",
		},
		{
			name:        "it should fail if the file doesn't exist (v1)",
			file:        "",
			f:           system.ReadCgroupMemLimitBytes,
			expectedErr: "open /proc/self/cgroup: file does not exist",
		},
		{
			name:      "it should read the mem limit (v2)",
			value:     "12345687",
			expected:  uint64(12345687),
			file:      "/redpanda.slice/redpanda.service/memory.max",
			f:         system.ReadCgroupMemLimitBytes,
			cgroupsV2: true,
		},
		{
			name:      "it should read the mem limit ('max') (v2)",
			value:     "max",
			expected:  math.MaxUint64,
			file:      "/redpanda.slice/redpanda.service/memory.max",
			f:         system.ReadCgroupMemLimitBytes,
			cgroupsV2: true,
		},
		{
			name:      "it should search recursively for the mem file (v2)",
			value:     "max",
			expected:  math.MaxUint64,
			file:      "/redpanda.slice/memory.max",
			f:         system.ReadCgroupMemLimitBytes,
			cgroupsV2: true,
		},
		{
			name:        "it should fail if the mem file is empty (v2)",
			value:       "",
			file:        "/redpanda.slice/redpanda.service/memory.max",
			f:           system.ReadCgroupMemLimitBytes,
			cgroupsV2:   true,
			expectedErr: "no value found in /sys/fs/cgroup/redpanda.slice/redpanda.service/memory.max",
		},
		{
			name:        "it should fail if the mem file doesn't exist (v2)",
			file:        "",
			f:           system.ReadCgroupMemLimitBytes,
			cgroupsV2:   true,
			expectedErr: "open /proc/self/cgroup: file does not exist",
		},
		{
			name:     "it should read the effective cpus (v1)",
			value:    "0-1",
			file:     "/cpuset/cpuset.effective_cpus",
			f:        system.ReadCgroupEffectiveCpusNo,
			expected: uint64(2),
		},
		{
			name:     "it should read the effective cpus (v1) (cpu ranges list)",
			value:    "0-1,3,6-10",
			file:     "/cpuset/cpuset.effective_cpus",
			f:        system.ReadCgroupEffectiveCpusNo,
			expected: uint64(8),
		},
		{
			name:        "it should fail if the cpu file is empty (v1)",
			value:       "",
			file:        "/cpuset/cpuset.effective_cpus",
			f:           system.ReadCgroupEffectiveCpusNo,
			expectedErr: "no value found in /sys/fs/cgroup/cpuset/cpuset.effective_cpus",
		},
		{
			name:        "it should fail if the cpu file doesn't exist (v1)",
			file:        "",
			f:           system.ReadCgroupEffectiveCpusNo,
			expectedErr: "open /proc/self/cgroup: file does not exist",
		},
		{
			name:        "it should fail if there's an invalid upper range limit in the cpulist (v1)",
			value:       "0-,3,6-10",
			file:        "/cpuset/cpuset.effective_cpus",
			f:           system.ReadCgroupEffectiveCpusNo,
			expectedErr: "couldn't parse effective CPU upper range limit '' in '0-,3,6-10'",
		},
		{
			name:        "it should fail if there's an invalid lower range limit in the cpulist (v1)",
			value:       "-1,3,6-10",
			file:        "/cpuset/cpuset.effective_cpus",
			f:           system.ReadCgroupEffectiveCpusNo,
			expectedErr: "couldn't parse effective CPU lower range limit '' in '-1,3,6-10'",
		},
		{
			name:        "it should fail if there's a missing value in the cpulist (v1)",
			value:       "0-2,,6-10",
			file:        "/cpuset/cpuset.effective_cpus",
			f:           system.ReadCgroupEffectiveCpusNo,
			expectedErr: "missing value in cpu list '0-2,,6-10'",
		},
		{
			name:      "it should read the effective cpus (v2)",
			value:     "3,5,8",
			file:      "/redpanda.slice/redpanda.service/cpuset.cpus.effective",
			f:         system.ReadCgroupEffectiveCpusNo,
			expected:  uint64(3),
			cgroupsV2: true,
		},
		{
			name:      "it should search recursively for the cpu file (v2)",
			value:     "0-5",
			file:      "/cpuset.cpus.effective",
			f:         system.ReadCgroupEffectiveCpusNo,
			expected:  uint64(6),
			cgroupsV2: true,
		},
		{
			name:        "it should fail if the value is empty (v2)",
			value:       "",
			file:        "/redpanda.slice/redpanda.service/cpuset.cpus.effective",
			f:           system.ReadCgroupEffectiveCpusNo,
			expectedErr: "no value found in /sys/fs/cgroup/redpanda.slice/redpanda.service/cpuset.cpus.effective",
			cgroupsV2:   true,
		},
		{
			name:        "it should fail if the cpu file is empty (v2)",
			file:        "",
			f:           system.ReadCgroupEffectiveCpusNo,
			expectedErr: "open /proc/self/cgroup: file does not exist",
			cgroupsV2:   true,
		},
		{
			name:        "it should fail if there's an invalid upper range limit in the cpulist (v2)",
			value:       "0-1,3,5,7-",
			file:        "/cpuset/cpuset.effective_cpus",
			f:           system.ReadCgroupEffectiveCpusNo,
			expectedErr: "couldn't parse effective CPU upper range limit '' in '0-1,3,5,7-'",
		},
		{
			name:        "it should fail if there's an invalid lower range limit in the cpulist (v2)",
			value:       "0-1,5,-10",
			file:        "/cpuset/cpuset.effective_cpus",
			f:           system.ReadCgroupEffectiveCpusNo,
			expectedErr: "couldn't parse effective CPU lower range limit '' in '0-1,5,-10'",
		},
		{
			name:        "it should fail if there's a missing value in the cpulist (v2)",
			value:       "0-2,6-10,",
			file:        "/cpuset/cpuset.effective_cpus",
			f:           system.ReadCgroupEffectiveCpusNo,
			expectedErr: "missing value in cpu list '0-2,6-10,'",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := afero.NewMemMapFs()
			err := setUpCgroup(fs, tt.file, tt.value, tt.cgroupsV2)
			assert.NoError(t, err)
			limit, err := tt.f(fs)
			if tt.expectedErr != "" {
				assert.EqualError(t, err, tt.expectedErr)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tt.expected, limit)
		})
	}
}

func TestReadCgroupNoCgroup(t *testing.T) {
	fs := afero.NewMemMapFs()
	funcs := []func(afero.Fs) (uint64, error){
		system.ReadCgroupMemLimitBytes,
		system.ReadCgroupEffectiveCpusNo,
	}
	for _, f := range funcs {
		_, err := f(fs)
		assert.EqualError(t, err, "open /proc/self/cgroup: file does not exist")
	}
}

func TestReadCgroupEmptyCgroup(t *testing.T) {
	fs := afero.NewMemMapFs()
	funcs := []func(afero.Fs) (uint64, error){
		system.ReadCgroupMemLimitBytes,
		system.ReadCgroupEffectiveCpusNo,
	}
	err := afero.WriteFile(fs, "/proc/self/cgroup", []byte{}, 0o644)
	assert.NoError(t, err)
	for _, f := range funcs {
		_, err := f(fs)
		assert.EqualError(t, err, "no cgroup data found for the current process")
	}
}
