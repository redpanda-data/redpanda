package disk

import (
	"fmt"
	"vectorized/pkg/checkers"

	"github.com/spf13/afero"
)

func CreateDirectoryCheckers(
	fs afero.Fs,
	dir string,
	infoProvider InfoProvider,
	newDeviceChecker func(string) checkers.Checker,
) ([]checkers.Checker, error) {
	devices, err := infoProvider.GetDirectoryDevices(dir)
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
	infoProvider InfoProvider,
) ([]checkers.Checker, error) {
	return CreateDirectoryCheckers(
		fs,
		dir,
		infoProvider,
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
	infoProvider InfoProvider,
) ([]checkers.Checker, error) {
	return CreateDirectoryCheckers(
		fs,
		dir,
		infoProvider,
		func(device string) checkers.Checker {
			return NewDeviceSchedulerChecker(fs, device, schedulerInfo)
		},
	)
}
