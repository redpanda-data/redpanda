package disk

import (
	"vectorized/pkg/tuners"
	"vectorized/pkg/utils"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
)

func NewDiskTuner(
	fs afero.Fs,
	directories []string,
	devices []string,
	blockDevices BlockDevices,
	deviceTunerFactory func(string) tuners.Tunable,
) tuners.Tunable {
	return &diskTuner{
		fs:                 fs,
		directories:        directories,
		devices:            devices,
		blockDevices:       blockDevices,
		deviceTunerFactory: deviceTunerFactory,
	}
}

type diskTuner struct {
	tuners.Tunable
	fs                 afero.Fs
	deviceTunerFactory func(string) tuners.Tunable
	blockDevices       BlockDevices
	directories        []string
	devices            []string
}

func (tuner *diskTuner) Tune() tuners.TuneResult {
	tunables, err := tuner.createDeviceTuners()
	if err != nil {
		return tuners.NewTuneError(err)
	}
	for _, tuner := range tunables {
		res := tuner.Tune()
		if res.IsFailed() {
			return res
		}
	}
	return tuners.NewTuneResult(false)
}

func (tuner *diskTuner) CheckIfSupported() (supported bool, reason string) {
	if len(tuner.directories) == 0 && len(tuner.devices) == 0 {
		return false,
			"Either direcories or devices must be provided for disk tuner"
	}
	tunables, err := tuner.createDeviceTuners()
	if err != nil {
		return false, err.Error()
	}
	for _, tuner := range tunables {
		supported, reason := tuner.CheckIfSupported()
		if supported == false {
			return false, reason
		}
	}
	return true, ""
}

func (tuner *diskTuner) createDeviceTuners() ([]tuners.Tunable, error) {
	directoryDevices, err := tuner.blockDevices.GetDirectoriesDevices(
		tuner.directories)
	if err != nil {
		return nil, err
	}
	disksSetMap := map[string]bool{}
	for _, devices := range directoryDevices {
		for _, device := range devices {
			disksSetMap[device] = true
		}
	}
	for _, device := range tuner.devices {
		disksSetMap[device] = true
	}
	devices := utils.GetKeys(disksSetMap)
	var tuners []tuners.Tunable
	for _, device := range devices {
		log.Debugf("Creating disk tuner for '%s'", device)
		tuners = append(tuners, tuner.deviceTunerFactory(device))
	}
	return tuners, nil
}
