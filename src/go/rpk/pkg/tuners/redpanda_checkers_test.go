package tuners_test

import (
	"path/filepath"
	"testing"
	"time"
	"vectorized/pkg/tuners"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
)

func setUpCgroup(fs afero.Fs, file, val string, v2 bool) error {
	if file == "" {
		return nil
	}
	fullPath := "/sys/fs/cgroup" + file
	dir := filepath.Dir(fullPath)
	if err := fs.MkdirAll("/proc/self/", 0755); err != nil {
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
		0644,
	)
	if err != nil {
		return err
	}
	if err = fs.MkdirAll(dir, 0755); err != nil {
		return err
	}
	return afero.WriteFile(fs, fullPath, []byte(val), 0644)
}

func setUpCgroupsV1(fs afero.Fs, file, val string) error {
	if err := fs.MkdirAll("/proc/self/", 0755); err != nil {
		return err
	}
	err := afero.WriteFile(
		fs,
		"/proc/self/cgroup",
		[]byte(`2:cpu,cpuacct:/user.slice
1:name=systemd:/user.slice/user-1.slice/user@1.service
`),
		0644,
	)
	if err != nil {
		return err
	}
	fullPath := "/sys/fs/cgroup" + file
	dir := filepath.Dir(fullPath)
	if err = fs.MkdirAll(dir, 0755); err != nil {
		return err
	}
	return afero.WriteFile(fs, fullPath, []byte(val), 0644)
}

func setUpCgroupsV2(fs afero.Fs, file, val string) error {
	cgroup := filepath.Dir(file)
	fullPath := "/sys/fs/cgroup" + file
	dir := filepath.Dir(fullPath)
	if err := fs.MkdirAll("/proc/self/", 0755); err != nil {
		return err
	}
	err := afero.WriteFile(
		fs,
		"/proc/self/cgroup",
		[]byte("0::"+cgroup),
		0644,
	)
	if err != nil {
		return err
	}
	if err = fs.MkdirAll(dir, 0755); err != nil {
		return err
	}
	return afero.WriteFile(fs, fullPath, []byte(val), 0644)
}

func TestNtpCheckTimeout(t *testing.T) {
	timeout := time.Duration(0)

	check := tuners.NewNTPSyncChecker(timeout, afero.NewMemMapFs())

	res := check.Check()

	if res.IsOk {
		t.Errorf("the NTP check shouldn't have succeeded")
	}
	if res.Err == nil {
		t.Errorf("the NTP check should have failed with an error")
	}
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
			expectedErr:   "no value found in /sys/fs/cgroup/memory/memory.limit_in_bytes",
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
			expectedErr:   "no value found in /sys/fs/cgroup/redpanda.slice/redpanda.service/memory.max",
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
				assert.NoError(t, err)
				err = setUpCgroup(
					fs,
					"/redpanda.slice/redpanda.service/cpuset.cpus.effective",
					tt.effectiveCpus,
					tt.cgroupsV2,
				)
				assert.NoError(t, err)
			} else {
				err := setUpCgroup(
					fs,
					"/memory/memory.limit_in_bytes",
					tt.memoryLimit,
					tt.cgroupsV2,
				)
				assert.NoError(t, err)
				err = setUpCgroup(
					fs,
					"/cpuset/cpuset.effective_cpus",
					tt.effectiveCpus,
					tt.cgroupsV2,
				)
				assert.NoError(t, err)
			}
			c := tuners.NewMemoryChecker(fs)
			res := c.Check()
			if tt.expectedErr != "" {
				assert.EqualError(t, res.Err, tt.expectedErr)
			} else {
				assert.NoError(t, res.Err, tt.name)
			}
			assert.Equal(t, tt.expectOk, res.IsOk)
		})
	}
}
