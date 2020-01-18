package tuners

import (
	"vectorized/pkg/tuners/disk"
	"vectorized/pkg/tuners/executors"
	"vectorized/pkg/tuners/executors/commands"

	"github.com/spf13/afero"
)

func NewDeviceNomergesTuner(
	fs afero.Fs,
	device string,
	schedulerInfo disk.SchedulerInfo,
	executor executors.Executor,
) Tunable {
	return NewCheckedTunable(
		NewDeviceNomergesChecker(fs, device, schedulerInfo),
		func() TuneResult {
			return tuneNomerges(fs, device, schedulerInfo, executor)
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
	schedulerInfo disk.SchedulerInfo,
	executor executors.Executor,
) TuneResult {
	featureFile, err := schedulerInfo.GetNomergesFeatureFile(device)
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
	schedulerInfo := disk.NewSchedulerInfo(fs, blockDevices)
	return NewDiskTuner(
		fs,
		directories,
		devices,
		blockDevices,
		func(device string) Tunable {
			return NewDeviceNomergesTuner(fs, device, schedulerInfo, executor)
		},
	)
}
