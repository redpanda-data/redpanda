// Copyright 2020 Vectorized, Inc.
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
	"vectorized/pkg/tuners/disk"
	"vectorized/pkg/tuners/irq"

	"github.com/spf13/afero"
)

func CreateDirectoryCheckers(
	fs afero.Fs,
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
	fs afero.Fs, device string, schedulerInfo disk.SchedulerInfo,
) Checker {
	return NewEqualityChecker(
		NomergesChecker,
		fmt.Sprintf("Disk '%s' nomerges tuned", device),
		Warning,
		true,
		func() (interface{}, error) {
			return checkDeviceNomerges(schedulerInfo, device)
		},
	)
}

func NewDirectoryNomergesChecker(
	fs afero.Fs,
	dir string,
	schedulerInfo disk.SchedulerInfo,
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
				ok, err := checkDeviceNomerges(schedulerInfo, device)
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
	schedulerInfo disk.SchedulerInfo, device string,
) (bool, error) {
	nomerges, err := schedulerInfo.GetNomerges(device)
	if err != nil {
		return false, err
	}
	return nomerges == 2, nil
}

func NewDeviceSchedulerChecker(
	fs afero.Fs, device string, schedulerInfo disk.SchedulerInfo,
) Checker {
	return NewEqualityChecker(
		SchedulerChecker,
		fmt.Sprintf("Disk '%s' scheduler tuned", device),
		Warning,
		true,
		func() (interface{}, error) {
			return checkScheduler(schedulerInfo, device)
		},
	)
}

func NewDirectorySchedulerChecker(
	fs afero.Fs,
	dir string,
	schedulerInfo disk.SchedulerInfo,
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
				ok, err := checkScheduler(schedulerInfo, device)
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
	schedulerInfo disk.SchedulerInfo, device string,
) (bool, error) {
	scheduler, err := schedulerInfo.GetScheduler(device)
	if err != nil {
		return false, err
	}
	return scheduler == "none" || scheduler == "noop", nil
}

func NewDisksIRQAffinityStaticChecker(
	fs afero.Fs,
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
	fs afero.Fs,
	dir string,
	blockDevices disk.BlockDevices,
	balanceService irq.BalanceService,
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
	fs afero.Fs,
	devices []string,
	cpuMask string,
	mode irq.Mode,
	blockDevices disk.BlockDevices,
	cpuMasks irq.CpuMasks,
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
	fs afero.Fs,
	dir string,
	cpuMask string,
	mode irq.Mode,
	blockDevices disk.BlockDevices,
	cpuMasks irq.CpuMasks,
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
	cpuMasks irq.CpuMasks,
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
