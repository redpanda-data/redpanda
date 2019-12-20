package disk

import (
	"vectorized/pkg/tuners"
	"vectorized/pkg/tuners/executors"
	"vectorized/pkg/tuners/executors/commands"

	"github.com/spf13/afero"
)

func NewDeviceNomergesTuner(
	fs afero.Fs,
	device string,
	schedulerInfo SchedulerInfo,
	executor executors.Executor,
) tuners.Tunable {
	return tuners.NewCheckedTunable(
		NewDeviceNomergesChecker(fs, device, schedulerInfo),
		func() tuners.TuneResult {
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
	schedulerInfo SchedulerInfo,
	executor executors.Executor,
) tuners.TuneResult {
	featureFile, err := schedulerInfo.GetNomergesFeatureFile(device)
	if err != nil {
		return tuners.NewTuneError(err)
	}
	err = executor.Execute(commands.NewWriteFileCmd(fs, featureFile, "2"))
	if err != nil {
		return tuners.NewTuneError(err)
	}

	return tuners.NewTuneResult(false)
}

func NewNomergesTuner(
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
			return NewDeviceNomergesTuner(fs, device, schedulerInfo, executor)
		},
	)
}
