package disk

import (
	"fmt"
	"vectorized/pkg/tuners"
	"vectorized/pkg/tuners/executors"
	"vectorized/pkg/tuners/executors/commands"

	"github.com/spf13/afero"
)

func NewDeviceSchedulerTuner(
	fs afero.Fs,
	device string,
	schedulerInfo SchedulerInfo,
	executor executors.Executor,
) tuners.Tunable {
	return tuners.NewCheckedTunable(
		NewDeviceSchedulerChecker(fs, device, schedulerInfo),
		func() tuners.TuneResult {
			return tuneScheduler(fs, device, schedulerInfo, executor)
		},
		func() (bool, string) {
			_, err := getPrefferedScheduler(device, schedulerInfo)
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
	schedulerInfo SchedulerInfo,
	executor executors.Executor,
) tuners.TuneResult {
	prefferedScheduler, err := getPrefferedScheduler(device, schedulerInfo)
	if err != nil {
		return tuners.NewTuneError(err)
	}
	featureFile, err := schedulerInfo.GetSchedulerFeatureFile(device)
	if err != nil {
		return tuners.NewTuneError(err)
	}
	err = executor.Execute(
		commands.NewWriteFileCmd(fs, featureFile, prefferedScheduler))
	if err != nil {
		return tuners.NewTuneError(err)
	}

	return tuners.NewTuneResult(false)
}

func getPrefferedScheduler(
	device string, schedulerInfo SchedulerInfo,
) (string, error) {
	supported, err := schedulerInfo.GetSupportedSchedulers(device)
	if err != nil {
		return "", err
	}
	preffered := []string{"none", "noop"}
	supportedMap := make(map[string]bool)

	for _, sched := range supported {
		supportedMap[sched] = true
	}

	for _, sched := range preffered {
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
	blockDevices BlockDevices,
	executor executors.Executor,
) tuners.Tunable {
	schedulerInfo := NewSchedulerInfo(fs, blockDevices)
	return NewDiskTuner(
		fs,
		directories,
		devices,
		blockDevices,
		func(device string) tuners.Tunable {
			return NewDeviceSchedulerTuner(fs, device, schedulerInfo, executor)
		},
	)
}
