package disk

import (
	"fmt"
	"vectorized/pkg/checkers"
	"vectorized/pkg/tuners/irq"

	"github.com/spf13/afero"
)

func CreateDirectoryCheckers(
	fs afero.Fs,
	dir string,
	blockDevices BlockDevices,
	newDeviceChecker func(string) checkers.Checker,
) ([]checkers.Checker, error) {
	devices, err := blockDevices.GetDirectoryDevices(dir)
	if err != nil {
		return nil, err
	}
	var checkers []checkers.Checker
	for _, device := range devices {
		checkers = append(checkers, newDeviceChecker(device))
	}
	return checkers, nil
}

func NewDeviceNomergesChecker(
	fs afero.Fs, device string, schedulerInfo SchedulerInfo,
) checkers.Checker {
	return checkers.NewEqualityChecker(
		fmt.Sprintf("Disk %s nomerges tuned", device),
		checkers.Warning,
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
	schedulerInfo SchedulerInfo,
	blockDevices BlockDevices,
) ([]checkers.Checker, error) {
	return CreateDirectoryCheckers(
		fs,
		dir,
		blockDevices,
		func(device string) checkers.Checker {
			return NewDeviceNomergesChecker(fs, device, schedulerInfo)
		},
	)
}

func NewDeviceSchedulerChecker(
	fs afero.Fs, device string, schedulerInfo SchedulerInfo,
) checkers.Checker {
	return checkers.NewEqualityChecker(
		fmt.Sprintf("Disk %s scheduler tuned", device),
		checkers.Warning,
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
	schedulerInfo SchedulerInfo,
	blockDevices BlockDevices,
) ([]checkers.Checker, error) {
	return CreateDirectoryCheckers(
		fs,
		dir,
		blockDevices,
		func(device string) checkers.Checker {
			return NewDeviceSchedulerChecker(fs, device, schedulerInfo)
		},
	)
}

func NewDisksIRQAffinityStaticChecker(
	fs afero.Fs,
	devices []string,
	blockDevices BlockDevices,
	balanceService irq.BalanceService,
) checkers.Checker {
	return checkers.NewEqualityChecker(
		"Disks IRQs affinity static",
		checkers.Warning,
		true,
		func() (interface{}, error) {
			IRQsMap, err := blockDevices.GetDevicesIRQs(devices)
			if err != nil {
				return false, err
			}
			return irq.AreIRQsStaticallyAssigned(
				irq.GetAllIRQs(IRQsMap), balanceService)
		},
	)
}

func NewDirectoryIRQsAffinityStaticChecker(
	fs afero.Fs,
	directory string,
	blockDevices BlockDevices,
	balanceService irq.BalanceService,
) (checkers.Checker, error) {
	devices, err := blockDevices.GetDirectoryDevices(directory)
	if err != nil {
		return nil, err
	}
	return NewDisksIRQAffinityStaticChecker(
		fs, devices, blockDevices, balanceService), nil
}
