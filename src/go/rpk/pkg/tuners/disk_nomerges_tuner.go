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
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/disk"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/executors"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/executors/commands"
	"github.com/spf13/afero"
)

func NewDeviceNomergesTuner(
	fs afero.Fs,
	device string,
	deviceFeatures disk.DeviceFeatures,
	executor executors.Executor,
) Tunable {
	return NewCheckedTunable(
		NewDeviceNomergesChecker(device, deviceFeatures),
		func() TuneResult {
			return tuneNomerges(fs, device, deviceFeatures, executor)
		},
		func() (bool, string) {
			return true, ""
		},
		executor.IsLazy(),
	)
}

func tuneNomerges(
	fs afero.Fs,
	device string,
	deviceFeatures disk.DeviceFeatures,
	executor executors.Executor,
) TuneResult {
	featureFile, err := deviceFeatures.GetNomergesFeatureFile(device)
	if err != nil {
		return NewTuneError(err)
	}
	err = executor.Execute(commands.NewWriteFileCmd(fs, featureFile, "2"))
	if err != nil {
		return NewTuneError(err)
	}

	return NewTuneResult(false)
}

func NewNomergesTuner(
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
			return NewDeviceNomergesTuner(fs, device, deviceFeatures, executor)
		},
	)
}
