package tuners

import (
	"fmt"
	"vectorized/pkg/tuners/disk"
	"vectorized/pkg/tuners/executors"
	"vectorized/pkg/tuners/executors/commands"

	"github.com/spf13/afero"
)

func NewDeviceSchedulerTuner(
	fs afero.Fs,
	device string,
	schedulerInfo disk.SchedulerInfo,
	executor executors.Executor,
) Tunable {
	return NewCheckedTunable(
		NewDeviceSchedulerChecker(fs, device, schedulerInfo),
		func() TuneResult {
			return tuneScheduler(fs, device, schedulerInfo, executor)
		},
		func() (bool, string) {
			_, err := getPreferredScheduler(device, schedulerInfo)
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
	schedulerInfo disk.SchedulerInfo,
	executor executors.Executor,
) TuneResult {
	preferredScheduler, err := getPreferredScheduler(device, schedulerInfo)
	if err != nil {
		return NewTuneError(err)
	}
	featureFile, err := schedulerInfo.GetSchedulerFeatureFile(device)
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
	device string, schedulerInfo disk.SchedulerInfo,
) (string, error) {
	supported, err := schedulerInfo.GetSupportedSchedulers(device)
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
	schedulerInfo := disk.NewSchedulerInfo(fs, blockDevices)
	return NewDiskTuner(
		fs,
		directories,
		devices,
		blockDevices,
		func(device string) Tunable {
			return NewDeviceSchedulerTuner(fs, device, schedulerInfo, executor)
		},
	)
}
