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
		fmt.Sprintf("Disk %s nomerges tuned", device),
		Warning,
		true,
		func() (interface{}, error) {
			nomerges, err := schedulerInfo.GetNomerges(device)
			if err != nil {
				return false, err
			}
			return nomerges == 2, nil
		},
	)
}

func NewDirectoryNomergesCheckers(
	fs afero.Fs,
	dir string,
	schedulerInfo disk.SchedulerInfo,
	blockDevices disk.BlockDevices,
) ([]Checker, error) {
	return CreateDirectoryCheckers(
		fs,
		dir,
		blockDevices,
		func(device string) Checker {
			return NewDeviceNomergesChecker(fs, device, schedulerInfo)
		},
	)
}

func NewDeviceSchedulerChecker(
	fs afero.Fs, device string, schedulerInfo disk.SchedulerInfo,
) Checker {
	return NewEqualityChecker(
		SchedulerChecker,
		fmt.Sprintf("Disk %s scheduler tuned", device),
		Warning,
		true,
		func() (interface{}, error) {
			scheduler, err := schedulerInfo.GetScheduler(device)
			if err != nil {
				return false, err
			}
			if scheduler == "none" || scheduler == "noop" {
				return true, nil
			}
			return false, nil
		},
	)
}

func NewDirectorySchedulerCheckers(
	fs afero.Fs,
	dir string,
	schedulerInfo disk.SchedulerInfo,
	blockDevices disk.BlockDevices,
) ([]Checker, error) {
	return CreateDirectoryCheckers(
		fs,
		dir,
		blockDevices,
		func(device string) Checker {
			return NewDeviceSchedulerChecker(fs, device, schedulerInfo)
		},
	)
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
			diskInfoByType, err := blockDevices.GetDiskInfoByType(devices)
			if err != nil {
				return false, err
			}
			var IRQs []int
			for _, diskInfo := range diskInfoByType {
				IRQs = append(IRQs, diskInfo.Irqs...)
			}
			return irq.AreIRQsStaticallyAssigned(IRQs, balanceService)
		},
	)
}

func NewDirectoryIRQsAffinityStaticChecker(
	fs afero.Fs,
	directory string,
	blockDevices disk.BlockDevices,
	balanceService irq.BalanceService,
) (Checker, error) {
	devices, err := blockDevices.GetDirectoryDevices(directory)
	if err != nil {
		return nil, err
	}
	return NewDisksIRQAffinityStaticChecker(
		fs, devices, blockDevices, balanceService), nil
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
		},
	)
}

func NewDirectoryIRQAffinityChecker(
	fs afero.Fs,
	directory string,
	cpuMask string,
	mode irq.Mode,
	blockDevices disk.BlockDevices,
	cpuMasks irq.CpuMasks,
) (Checker, error) {
	devices, err := blockDevices.GetDirectoryDevices(directory)
	if err != nil {
		return nil, err
	}
	return NewDisksIRQAffinityChecker(
		fs, devices, cpuMask, mode, blockDevices, cpuMasks), nil
}
