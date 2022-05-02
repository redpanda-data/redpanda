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
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/irq"
	"github.com/spf13/afero"
)

func CreateDirectoryCheckers(
	dir string,
	blockDevices disk.BlockDevices,
	newDeviceChecker func(string) Checker,
) ([]Checker, error) {
	devices, err := blockDevices.GetDirectoryDevices(dir)
	if err != nil {
		return nil, err
	}
	var checkers []Checker
	for _, device := range devices {
		checkers = append(checkers, newDeviceChecker(device))
	}
	return checkers, nil
}

func NewDeviceNomergesChecker(
	device string, deviceFeatures disk.DeviceFeatures,
) Checker {
	return NewEqualityChecker(
		NomergesChecker,
		fmt.Sprintf("Disk '%s' nomerges tuned", device),
		Warning,
		true,
		func() (interface{}, error) {
			return checkDeviceNomerges(deviceFeatures, device)
		},
	)
}

func NewDirectoryNomergesChecker(
	dir string,
	deviceFeatures disk.DeviceFeatures,
	blockDevices disk.BlockDevices,
) Checker {
	return NewEqualityChecker(
		NomergesChecker,
		fmt.Sprintf("Dir '%s' nomerges tuned", dir),
		Warning,
		true,
		func() (interface{}, error) {
			devices, err := blockDevices.GetDirectoryDevices(dir)
			if err != nil {
				return false, err
			}
			tuned := true
			for _, device := range devices {
				ok, err := checkDeviceNomerges(deviceFeatures, device)
				if err != nil {
					return false, err
				}
				tuned = tuned && ok
			}
			return tuned, nil
		},
	)
}

func checkDeviceNomerges(
	deviceFeatures disk.DeviceFeatures, device string,
) (bool, error) {
	nomerges, err := deviceFeatures.GetNomerges(device)
	if err != nil {
		return false, err
	}
	return nomerges == 2, nil
}

func NewDeviceSchedulerChecker(
	_ afero.Fs, device string, deviceFeatures disk.DeviceFeatures,
) Checker {
	return NewEqualityChecker(
		SchedulerChecker,
		fmt.Sprintf("Disk '%s' scheduler tuned", device),
		Warning,
		true,
		func() (interface{}, error) {
			return checkScheduler(deviceFeatures, device)
		},
	)
}

func NewDirectorySchedulerChecker(
	dir string,
	deviceFeatures disk.DeviceFeatures,
	blockDevices disk.BlockDevices,
) Checker {
	return NewEqualityChecker(
		SchedulerChecker,
		fmt.Sprintf("Dir '%s' scheduler tuned", dir),
		Warning,
		true,
		func() (interface{}, error) {
			devices, err := blockDevices.GetDirectoryDevices(dir)
			if err != nil {
				return nil, err
			}
			tuned := true
			for _, device := range devices {
				ok, err := checkScheduler(deviceFeatures, device)
				if err != nil {
					return false, err
				}
				tuned = tuned && ok
			}
			return tuned, nil
		},
	)
}

func checkScheduler(
	deviceFeatures disk.DeviceFeatures, device string,
) (bool, error) {
	scheduler, err := deviceFeatures.GetScheduler(device)
	if err != nil {
		return false, err
	}
	return scheduler == "none" || scheduler == "noop", nil
}

func NewDeviceWriteCacheChecker(
	device string, deviceFeatures disk.DeviceFeatures,
) Checker {
	return NewEqualityChecker(
		SchedulerChecker,
		fmt.Sprintf("Disk '%s' write cache tuned", device),
		Warning,
		true,
		func() (interface{}, error) {
			return checkDeviceWriteCache(deviceFeatures, device)
		},
	)
}

func NewDirectoryWriteCacheChecker(
	dir string,
	deviceFeatures disk.DeviceFeatures,
	blockDevices disk.BlockDevices,
) Checker {
	return NewEqualityChecker(
		SchedulerChecker,
		fmt.Sprintf("Dir '%s' write cache tuned", dir),
		Warning,
		true,
		func() (interface{}, error) {
			devices, err := blockDevices.GetDirectoryDevices(dir)
			if err != nil {
				return nil, err
			}
			tuned := true
			for _, device := range devices {
				ok, err := checkDeviceWriteCache(deviceFeatures, device)
				if err != nil {
					return false, err
				}
				tuned = tuned && ok
			}
			return tuned, nil
		},
	)
}

func checkDeviceWriteCache(
	deviceFeatures disk.DeviceFeatures, device string,
) (bool, error) {
	cachePolicy, err := deviceFeatures.GetWriteCache(device)
	if err != nil {
		return false, err
	}
	return (cachePolicy == disk.CachePolicyWriteThrough), nil
}

func NewDisksIRQAffinityStaticChecker(
	devices []string,
	blockDevices disk.BlockDevices,
	balanceService irq.BalanceService,
) Checker {
	return NewEqualityChecker(
		DiskIRQsAffinityStaticChecker,
		"Disks IRQs affinity static",
		Warning,
		true,
		func() (interface{}, error) {
			return checkDisksIRQsAffinity(blockDevices, balanceService, devices)
		},
	)
}

func NewDirectoryIRQsAffinityStaticChecker(
	dir string, blockDevices disk.BlockDevices, balanceService irq.BalanceService,
) Checker {
	return NewEqualityChecker(
		DiskIRQsAffinityStaticChecker,
		fmt.Sprintf("Dir '%s' IRQs affinity static", dir),
		Warning,
		true,
		func() (interface{}, error) {
			devices, err := blockDevices.GetDirectoryDevices(dir)
			if err != nil {
				return nil, err
			}
			return checkDisksIRQsAffinity(
				blockDevices,
				balanceService,
				devices,
			)
		},
	)
}

func checkDisksIRQsAffinity(
	blockDevices disk.BlockDevices,
	balanceService irq.BalanceService,
	devices []string,
) (bool, error) {
	diskInfoByType, err := blockDevices.GetDiskInfoByType(devices)
	if err != nil {
		return false, err
	}
	var IRQs []int
	for _, diskInfo := range diskInfoByType {
		IRQs = append(IRQs, diskInfo.Irqs...)
	}
	return irq.AreIRQsStaticallyAssigned(IRQs, balanceService)
}

func NewDisksIRQAffinityChecker(
	devices []string,
	cpuMask string,
	mode irq.Mode,
	blockDevices disk.BlockDevices,
	cpuMasks irq.CPUMasks,
) Checker {
	return NewEqualityChecker(
		DiskIRQsAffinityChecker,
		"Disks IRQs affinity set",
		Warning,
		true,
		func() (interface{}, error) {
			return areDevicesIRQsDistributed(
				devices,
				cpuMask,
				mode,
				blockDevices,
				cpuMasks,
			)
		},
	)
}

func NewDirectoryIRQAffinityChecker(
	dir string,
	cpuMask string,
	mode irq.Mode,
	blockDevices disk.BlockDevices,
	cpuMasks irq.CPUMasks,
) Checker {
	return NewEqualityChecker(
		DiskIRQsAffinityChecker,
		fmt.Sprintf("Dir '%s' IRQs affinity set", dir),
		Warning,
		true,
		func() (interface{}, error) {
			devices, err := blockDevices.GetDirectoryDevices(dir)
			if err != nil {
				return false, err
			}
			return areDevicesIRQsDistributed(
				devices,
				cpuMask,
				mode,
				blockDevices,
				cpuMasks,
			)
		},
	)
}

func areDevicesIRQsDistributed(
	devices []string,
	cpuMask string,
	mode irq.Mode,
	blockDevices disk.BlockDevices,
	cpuMasks irq.CPUMasks,
) (bool, error) {
	expectedDistribution, err := GetExpectedIRQsDistribution(
		devices,
		blockDevices,
		mode,
		cpuMask,
		cpuMasks)
	if err != nil {
		return false, err
	}

	for IRQ, mask := range expectedDistribution {
		readMask, err := cpuMasks.ReadIRQMask(IRQ)
		if err != nil {
			return false, err
		}
		eq, err := irq.MasksEqual(mask, readMask)
		if err != nil {
			return false, err
		}
		if !eq {
			return false, nil
		}
	}
	return true, nil
}
