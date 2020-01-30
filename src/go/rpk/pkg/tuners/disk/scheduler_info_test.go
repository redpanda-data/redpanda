package disk

import (
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
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

func TestSchedulerInfo_GetScheduler(t *testing.T) {
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
	schedulerInfo := NewSchedulerInfo(fs, blockDevices)
	// when
	scheduler, err := schedulerInfo.GetScheduler("fake")
	// then
	assert.NoError(t, err)
	assert.Equal(t, "noop", scheduler)
}

func TestSchedulerInfo_GetSupportedScheduler(t *testing.T) {
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
	schedulerInfo := NewSchedulerInfo(fs, blockDevices)
	// when
	schedulers, err := schedulerInfo.GetSupportedSchedulers("fake")
	// then
	assert.NoError(t, err)
	assert.Contains(t, schedulers, "noop")
	assert.Contains(t, schedulers, "deadline")
	assert.Contains(t, schedulers, "cfq")
	assert.Len(t, schedulers, 3)
}

func TestSchedulerInfo_GetNoMerges(t *testing.T) {
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
	schedulerInfo := NewSchedulerInfo(fs, blockDevices)
	// when
	nomerges, err := schedulerInfo.GetNomerges("fake")
	// then
	assert.NoError(t, err)
	assert.Equal(t, nomerges, 2)
}
