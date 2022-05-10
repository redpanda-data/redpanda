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
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/disk"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/executors"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

const (
	deadline   string = "deadline"
	fScheduler string = "/sys/devices/pci0000:00/0000:00:1d.0/0000:71:00.0/nvme/fake/queue/scheduler"
)

type deviceFeaturesMock struct {
	disk.DeviceFeatures
	getSupportedSchedulers   func(string) ([]string, error)
	getNomerges              func(string) (int, error)
	getNomergesFeatureFile   func(string) (string, error)
	getSchedulerFeatureFile  func(string) (string, error)
	getScheduler             func(string) (string, error)
	getWriteCacheFeatureFile func(string) (string, error)
	getWriteCache            func(string) (string, error)
}

func (m *deviceFeaturesMock) GetScheduler(device string) (string, error) {
	return m.getScheduler(device)
}

func (m *deviceFeaturesMock) GetSupportedSchedulers(
	device string,
) ([]string, error) {
	return m.getSupportedSchedulers(device)
}

func (m *deviceFeaturesMock) GetNomerges(device string) (int, error) {
	return m.getNomerges(device)
}

func (m *deviceFeaturesMock) GetNomergesFeatureFile(
	device string,
) (string, error) {
	return m.getNomergesFeatureFile(device)
}

func (m *deviceFeaturesMock) GetSchedulerFeatureFile(
	device string,
) (string, error) {
	return m.getSchedulerFeatureFile(device)
}

func (m *deviceFeaturesMock) GetWriteCacheFeatureFile(
	device string,
) (string, error) {
	return m.getWriteCacheFeatureFile(device)
}

func (m *deviceFeaturesMock) GetWriteCache(device string) (string, error) {
	return m.getWriteCache(device)
}

func TestDeviceSchedulerTuner_Tune(t *testing.T) {
	// given
	deviceFeatures := &deviceFeaturesMock{
		getSchedulerFeatureFile: func(string) (string, error) {
			return fScheduler, nil
		},
		getScheduler: func(string) (string, error) {
			return deadline, nil
		},
		getSupportedSchedulers: func(string) ([]string, error) {
			return []string{"deadline", "cfq", "noop"}, nil
		},
	}
	fs := afero.NewMemMapFs()
	fs.MkdirAll("/sys/devices/pci0000:00/0000:00:1d.0/0000:71:00.0/nvme/fake/queue", 0o644)
	tuner := NewDeviceSchedulerTuner(fs, "fake", deviceFeatures, executors.NewDirectExecutor())
	// when
	tuner.Tune()
	// then
	setValue, _ := afero.ReadFile(fs, fScheduler)
	require.Equal(t, "noop", string(setValue))
}

func TestDeviceSchedulerTuner_IsSupported_Should_return_true(t *testing.T) {
	// given
	deviceFeatures := &deviceFeaturesMock{
		getSchedulerFeatureFile: func(string) (string, error) {
			return fScheduler, nil
		},
		getScheduler: func(string) (string, error) {
			return deadline, nil
		},
		getSupportedSchedulers: func(string) ([]string, error) {
			return []string{"deadline", "cfq", "noop"}, nil
		},
	}
	fs := afero.NewMemMapFs()
	fs.MkdirAll("/sys/devices/pci0000:00/0000:00:1d.0/0000:71:00.0/nvme/fake/queue", 0o644)
	tuner := NewDeviceSchedulerTuner(fs, "fake", deviceFeatures, executors.NewDirectExecutor())
	// when
	supported, _ := tuner.CheckIfSupported()
	// then
	require.True(t, supported)
}

func TestDeviceSchedulerTuner_IsSupported_should_return_false(t *testing.T) {
	// given
	deviceFeatures := &deviceFeaturesMock{
		getSchedulerFeatureFile: func(string) (string, error) {
			return fScheduler, nil
		},
		getScheduler: func(string) (string, error) {
			return deadline, nil
		},
		getSupportedSchedulers: func(string) ([]string, error) {
			return []string{"deadline", "cfq"}, nil
		},
	}
	fs := afero.NewMemMapFs()
	fs.MkdirAll("/sys/devices/pci0000:00/0000:00:1d.0/0000:71:00.0/nvme/fake/queue", 0o644)
	tuner := NewDeviceSchedulerTuner(fs, "fake", deviceFeatures, executors.NewDirectExecutor())
	// when
	supported, _ := tuner.CheckIfSupported()
	// then
	require.False(t, supported)
}

func TestDeviceSchedulerTuner_Tune_should_prefer_none_over_noop(t *testing.T) {
	// given
	deviceFeatures := &deviceFeaturesMock{
		getSchedulerFeatureFile: func(string) (string, error) {
			return fScheduler, nil
		},
		getScheduler: func(string) (string, error) {
			return deadline, nil
		},
		getSupportedSchedulers: func(string) ([]string, error) {
			return []string{"deadline", "cfq", "noop", "none"}, nil
		},
	}
	fs := afero.NewMemMapFs()
	fs.MkdirAll("/sys/devices/pci0000:00/0000:00:1d.0/0000:71:00.0/nvme/fake/queue", 0o644)
	tuner := NewDeviceSchedulerTuner(fs, "fake", deviceFeatures, executors.NewDirectExecutor())
	// when
	tuner.Tune()
	// then
	setValue, _ := afero.ReadFile(fs, fScheduler)
	require.Equal(t, "none", string(setValue))
}
