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
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cloud/gcp"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cloud/vendor"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/disk"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/executors"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/executors/commands"
	"github.com/spf13/afero"
)

func NewGcpWriteCacheTuner(
	fs afero.Fs,
	directories []string,
	devices []string,
	blockDevices disk.BlockDevices,
	vendor vendor.Vendor,
	executor executors.Executor,
) Tunable {
	deviceFeatures := disk.NewDeviceFeatures(fs, blockDevices)
	return NewDiskTuner(
		fs,
		directories,
		devices,
		blockDevices,
		func(device string) Tunable {
			return NewDeviceGcpWriteCacheTuner(fs, device, deviceFeatures,
				vendor, executor)
		},
	)
}

func NewDeviceGcpWriteCacheTuner(
	fs afero.Fs,
	device string,
	deviceFeatures disk.DeviceFeatures,
	vendor vendor.Vendor,
	executor executors.Executor,
) Tunable {
	return NewCheckedTunable(
		NewDeviceWriteCacheChecker(device, deviceFeatures),
		func() TuneResult {
			return tuneWriteCache(fs, device, deviceFeatures, executor)
		},
		func() (bool, string) {
			v, err := vendor.Init()
			if err != nil {
				return false, "Disk write cache tuner is only supported in GCP"
			}
			gcpVendor := gcp.GcpVendor{}
			return v.Name() == gcpVendor.Name(), ""
		},
		executor.IsLazy(),
	)
}

func tuneWriteCache(
	fs afero.Fs,
	device string,
	deviceFeatures disk.DeviceFeatures,
	executor executors.Executor,
) TuneResult {
	featureFile, err := deviceFeatures.GetWriteCacheFeatureFile(device)
	if err != nil {
		return NewTuneError(err)
	}
	err = executor.Execute(
		commands.NewWriteFileCmd(fs, featureFile, disk.CachePolicyWriteThrough))

	if err != nil {
		return NewTuneError(err)
	}

	return NewTuneResult(false)
}
