package disk

import (
	"testing"
	"vectorized/pkg/tuners/executors"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
)

type schedulerInfoMock struct {
	SchedulerInfo
	getSupportedSchedulers  func(string) ([]string, error)
	getNomerges             func(string) (int, error)
	getNomergesFeatureFile  func(string) (string, error)
	getSchedulerFeatureFile func(string) (string, error)
	getScheduler            func(string) (string, error)
}

func (m *schedulerInfoMock) GetScheduler(device string) (string, error) {
	return m.getScheduler(device)
}

func (m *schedulerInfoMock) GetSupportedSchedulers(
	device string,
) ([]string, error) {
	return m.getSupportedSchedulers(device)
}

func (m *schedulerInfoMock) GetNomerges(device string) (int, error) {
	return m.getNomerges(device)
}

func (m *schedulerInfoMock) GetNomergesFeatureFile(
	device string,
) (string, error) {
	return m.getNomergesFeatureFile(device)
}

func (m *schedulerInfoMock) GetSchedulerFeatureFile(
	device string,
) (string, error) {
	return m.getSchedulerFeatureFile(device)
}

func TestDeviceSchedulerTuner_Tune(t *testing.T) {
	// given
	schedulerInfo := &schedulerInfoMock{
		getSchedulerFeatureFile: func(string) (string, error) {
			return "/sys/devices/pci0000:00/0000:00:1d.0/0000:71:00.0/nvme/fake/queue/scheduler", nil
		},
		getScheduler: func(string) (string, error) {
			return "deadline", nil
		},
		getSupportedSchedulers: func(string) ([]string, error) {
			return []string{"deadline", "cfq", "noop"}, nil
		},
	}
	fs := afero.NewMemMapFs()
	fs.MkdirAll("/sys/devices/pci0000:00/0000:00:1d.0/0000:71:00.0/nvme/fake/queue", 0644)
	tuner := NewDeviceSchedulerTuner(fs, "fake", schedulerInfo, executors.NewDirectExecutor())
	// when
	tuner.Tune()
	// then
	setValue, _ := afero.ReadFile(fs, "/sys/devices/pci0000:00/0000:00:1d.0/0000:71:00.0/nvme/fake/queue/scheduler")
	assert.Equal(t, "noop", string(setValue))
}

func TestDeviceSchedulerTuner_IsSupported_Should_return_true(t *testing.T) {
	// given
	schedulerInfo := &schedulerInfoMock{
		getSchedulerFeatureFile: func(string) (string, error) {
			return "/sys/devices/pci0000:00/0000:00:1d.0/0000:71:00.0/nvme/fake/queue/scheduler", nil
		},
		getScheduler: func(string) (string, error) {
			return "deadline", nil
		},
		getSupportedSchedulers: func(string) ([]string, error) {
			return []string{"deadline", "cfq", "noop"}, nil
		},
	}
	fs := afero.NewMemMapFs()
	fs.MkdirAll("/sys/devices/pci0000:00/0000:00:1d.0/0000:71:00.0/nvme/fake/queue", 0644)
	tuner := NewDeviceSchedulerTuner(fs, "fake", schedulerInfo, executors.NewDirectExecutor())
	// when
	supported, _ := tuner.CheckIfSupported()
	// then
	assert.True(t, supported)
}

func TestDeviceSchedulerTuner_IsSupported_should_return_false(t *testing.T) {
	// given
	schedulerInfo := &schedulerInfoMock{
		getSchedulerFeatureFile: func(string) (string, error) {
			return "/sys/devices/pci0000:00/0000:00:1d.0/0000:71:00.0/nvme/fake/queue/scheduler", nil
		},
		getScheduler: func(string) (string, error) {
			return "deadline", nil
		},
		getSupportedSchedulers: func(string) ([]string, error) {
			return []string{"deadline", "cfq"}, nil
		},
	}
	fs := afero.NewMemMapFs()
	fs.MkdirAll("/sys/devices/pci0000:00/0000:00:1d.0/0000:71:00.0/nvme/fake/queue", 0644)
	tuner := NewDeviceSchedulerTuner(fs, "fake", schedulerInfo, executors.NewDirectExecutor())
	// when
	supported, _ := tuner.CheckIfSupported()
	// then
	assert.False(t, supported)
}

func TestDeviceSchedulerTuner_Tune_should_prefer_none_over_noop(t *testing.T) {
	// given
	schedulerInfo := &schedulerInfoMock{
		getSchedulerFeatureFile: func(string) (string, error) {
			return "/sys/devices/pci0000:00/0000:00:1d.0/0000:71:00.0/nvme/fake/queue/scheduler", nil
		},
		getScheduler: func(string) (string, error) {
			return "deadline", nil
		},
		getSupportedSchedulers: func(string) ([]string, error) {
			return []string{"deadline", "cfq", "noop", "none"}, nil
		},
	}
	fs := afero.NewMemMapFs()
	fs.MkdirAll("/sys/devices/pci0000:00/0000:00:1d.0/0000:71:00.0/nvme/fake/queue", 0644)
	tuner := NewDeviceSchedulerTuner(fs, "fake", schedulerInfo, executors.NewDirectExecutor())
	// when
	tuner.Tune()
	// then
	setValue, _ := afero.ReadFile(fs, "/sys/devices/pci0000:00/0000:00:1d.0/0000:71:00.0/nvme/fake/queue/scheduler")
	assert.Equal(t, "none", string(setValue))
}
