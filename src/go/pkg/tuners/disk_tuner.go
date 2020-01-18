package tuners

import (
	"vectorized/pkg/tuners/disk"
	"vectorized/pkg/utils"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
)

func NewDiskTuner(
	fs afero.Fs,
	directories []string,
	devices []string,
	blockDevices disk.BlockDevices,
	deviceTunerFactory func(string) Tunable,
) Tunable {
	return &diskTuner{
		fs:                 fs,
		directories:        directories,
		devices:            devices,
		blockDevices:       blockDevices,
		deviceTunerFactory: deviceTunerFactory,
	}
}

type diskTuner struct {
	fs                 afero.Fs
	deviceTunerFactory func(string) Tunable
	blockDevices       disk.BlockDevices
	directories        []string
	devices            []string
}

func (tuner *diskTuner) Tune() TuneResult {
	tunables, err := tuner.createDeviceTuners()
	if err != nil {
		return NewTuneError(err)
	}
	return NewAggregatedTunable(tunables).Tune()
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
	return NewAggregatedTunable(tunables).CheckIfSupported()
}

func (tuner *diskTuner) createDeviceTuners() ([]Tunable, error) {
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
	var tuners []Tunable
	for _, device := range devices {
		log.Debugf("Creating disk tuner for '%s'", device)
		tuners = append(tuners, tuner.deviceTunerFactory(device))
	}
	return tuners, nil
}
