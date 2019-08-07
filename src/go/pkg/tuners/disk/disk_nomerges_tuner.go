package disk

import (
	"vectorized/pkg/tuners"

	"github.com/spf13/afero"
)

func NewDeviceNomergesTuner(
	fs afero.Fs, device string, schedulerInfo SchedulerInfo,
) tuners.Tunable {
	return tuners.NewCheckedTunable(
		NewDeviceNomergesChecker(fs, device, schedulerInfo),
		func() tuners.TuneResult {
			return tuneNomerges(fs, device, schedulerInfo)
		},
		func() (bool, string) {
			return true, ""
		},
	)
}

func tuneNomerges(
	fs afero.Fs, device string, schedulerInfo SchedulerInfo,
) tuners.TuneResult {
	featureFile, err := schedulerInfo.GetNomergesFeatureFile(device)
	if err != nil {
		return tuners.NewTuneError(err)
	}
	err = afero.WriteFile(fs, featureFile, []byte("2"), 0644)
	if err != nil {
		return tuners.NewTuneError(err)
	}

	return tuners.NewTuneResult(false)
}

type nomergesTuner struct {
	tuners.Tunable
	blockDevices BlockDevices
	directories  []string
	devices      []string
	fs           afero.Fs
}

func NewNomergesTuner(
	fs afero.Fs,
	directories []string,
	devices []string,
	blockDevices BlockDevices,
) tuners.Tunable {
	schedulerInfo := NewSchedulerInfo(fs, blockDevices)
	return NewDiskTuner(
		fs,
		directories,
		devices,
		blockDevices,
		func(device string) tuners.Tunable {
			return NewDeviceNomergesTuner(fs, device, schedulerInfo)
		},
	)
}
