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
	"fmt"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/disk"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/executors"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/executors/commands"
	"github.com/spf13/afero"
)

func NewDeviceSchedulerTuner(
	fs afero.Fs,
	device string,
	deviceFeatures disk.DeviceFeatures,
	executor executors.Executor,
) Tunable {
	return NewCheckedTunable(
		NewDeviceSchedulerChecker(fs, device, deviceFeatures),
		func() TuneResult {
			return tuneScheduler(fs, device, deviceFeatures, executor)
		},
		func() (bool, string) {
			_, err := getPreferredScheduler(device, deviceFeatures)
			if err != nil {
				return false, err.Error()
			}
			return true, ""
		},
		executor.IsLazy(),
	)
}

func tuneScheduler(
	fs afero.Fs,
	device string,
	deviceFeatures disk.DeviceFeatures,
	executor executors.Executor,
) TuneResult {
	preferredScheduler, err := getPreferredScheduler(device, deviceFeatures)
	if err != nil {
		return NewTuneError(err)
	}
	featureFile, err := deviceFeatures.GetSchedulerFeatureFile(device)
	if err != nil {
		return NewTuneError(err)
	}
	err = executor.Execute(
		commands.NewWriteFileCmd(fs, featureFile, preferredScheduler))
	if err != nil {
		return NewTuneError(err)
	}

	return NewTuneResult(false)
}

func getPreferredScheduler(
	device string, deviceFeatures disk.DeviceFeatures,
) (string, error) {
	supported, err := deviceFeatures.GetSupportedSchedulers(device)
	if err != nil {
		return "", err
	}
	preferred := []string{"none", "noop"}
	supportedMap := make(map[string]bool)

	for _, sched := range supported {
		supportedMap[sched] = true
	}

	for _, sched := range preferred {
		if _, exists := supportedMap[sched]; exists {
			return sched, nil
		}
	}
	return "", fmt.Errorf("None and Noop schedulers are not supported for %s",
		device)
}

func NewSchedulerTuner(
	fs afero.Fs,
	directories []string,
	devices []string,
	blockDevices disk.BlockDevices,
	executor executors.Executor,
) Tunable {
	deviceFeatures := disk.NewDeviceFeatures(fs, blockDevices)
	return NewDiskTuner(
		fs,
		directories,
		devices,
		blockDevices,
		func(device string) Tunable {
			return NewDeviceSchedulerTuner(fs, device, deviceFeatures, executor)
		},
	)
}
