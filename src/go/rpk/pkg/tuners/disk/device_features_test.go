// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package disk

import (
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

type blockDevicesMock struct {
	getDirectoriesDevices    func([]string) (map[string][]string, error)
	getDirectoryDevices      func(string) ([]string, error)
	getBlockDeviceFromPath   func(string) (BlockDevice, error)
	getBlockDeviceSystemPath func(string) (string, error)
	getDiskInfoByType        func([]string) (map[DiskType]DevicesIRQs, error)
}

func (m *blockDevicesMock) GetDirectoriesDevices(
	directories []string,
) (map[string][]string, error) {
	return m.getDirectoriesDevices(directories)
}

func (m *blockDevicesMock) GetDeviceFromPath(path string) (BlockDevice, error) {
	return m.getBlockDeviceFromPath(path)
}

func (m *blockDevicesMock) GetDeviceSystemPath(path string) (string, error) {
	return m.getBlockDeviceSystemPath(path)
}

func (m *blockDevicesMock) GetDirectoryDevices(path string) ([]string, error) {
	return m.getDirectoryDevices(path)
}

func (m *blockDevicesMock) GetDiskInfoByType(
	devices []string,
) (map[DiskType]DevicesIRQs, error) {
	return m.getDiskInfoByType(devices)
}

var noopSchedulerEnabled = "deadline cfq [noop]"

func TestDeviceFeatures_GetScheduler(t *testing.T) {
	// given
	blockDevices := &blockDevicesMock{
		getBlockDeviceFromPath: func(path string) (BlockDevice, error) {
			return &blockDevice{
				devnode: "/dev/fake",
				syspath: "/sys/devices/pci0000:00/0000:00:1d.0/0000:71:00.0/nvme/fake",
			}, nil
		},
	}
	fs := afero.NewMemMapFs()
	fs.MkdirAll("/sys/devices/pci0000:00/0000:00:1d.0/0000:71:00.0/nvme/fake/queue", 0644)
	afero.WriteFile(fs,
		"/sys/devices/pci0000:00/0000:00:1d.0/0000:71:00.0/nvme/fake/queue/scheduler",
		[]byte(noopSchedulerEnabled), 0644)
	deviceFeatures := NewDeviceFeatures(fs, blockDevices)
	// when
	scheduler, err := deviceFeatures.GetScheduler("fake")
	// then
	require.NoError(t, err)
	require.Equal(t, "noop", scheduler)
}

func TestDeviceFeatures_GetSupportedScheduler(t *testing.T) {
	// given
	blockDevices := &blockDevicesMock{
		getBlockDeviceFromPath: func(path string) (BlockDevice, error) {
			return &blockDevice{
				devnode: "/dev/fake",
				syspath: "/sys/devices/pci0000:00/0000:00:1d.0/0000:71:00.0/nvme/fake",
			}, nil
		},
	}
	fs := afero.NewMemMapFs()
	fs.MkdirAll("/sys/devices/pci0000:00/0000:00:1d.0/0000:71:00.0/nvme/fake/queue", 0644)
	afero.WriteFile(fs,
		"/sys/devices/pci0000:00/0000:00:1d.0/0000:71:00.0/nvme/fake/queue/scheduler",
		[]byte(noopSchedulerEnabled), 0644)
	deviceFeatures := NewDeviceFeatures(fs, blockDevices)
	// when
	schedulers, err := deviceFeatures.GetSupportedSchedulers("fake")
	// then
	require.NoError(t, err)
	require.Contains(t, schedulers, "noop")
	require.Contains(t, schedulers, "deadline")
	require.Contains(t, schedulers, "cfq")
	require.Len(t, schedulers, 3)
}

func TestDeviceFeatures_GetNoMerges(t *testing.T) {
	// given
	blockDevices := &blockDevicesMock{
		getBlockDeviceFromPath: func(path string) (BlockDevice, error) {
			return &blockDevice{
				devnode: "/dev/fake",
				syspath: "/sys/devices/pci0000:00/0000:00:1d.0/0000:71:00.0/nvme/fake",
			}, nil
		},
	}
	fs := afero.NewMemMapFs()
	fs.MkdirAll("/sys/devices/pci0000:00/0000:00:1d.0/0000:71:00.0/nvme/fake/queue", 0644)
	afero.WriteFile(fs,
		"/sys/devices/pci0000:00/0000:00:1d.0/0000:71:00.0/nvme/fake/queue/nomerges",
		[]byte("2"), 0644)
	deviceFeatures := NewDeviceFeatures(fs, blockDevices)
	// when
	nomerges, err := deviceFeatures.GetNomerges("fake")
	// then
	require.NoError(t, err)
	require.Equal(t, nomerges, 2)
}
