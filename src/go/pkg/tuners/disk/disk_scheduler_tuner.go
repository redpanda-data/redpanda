package disk

import (
	"fmt"
	"io/ioutil"
	"path"
	"strconv"
	"strings"
	"vectorized/tuners"
	"vectorized/utils"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
)

type SchedulerTuner struct {
	tuners.Tunable
	diskInfoProvider InfoProvider
	directories      []string
	devices          []string
	ioSchedulers     []string
	noMerges         int
	fs               afero.Fs
}

func NewSchedulerTuner(
	directories []string,
	devices []string,
	diskInfoProvider InfoProvider,
	fs afero.Fs,
) tuners.Tunable {
	return &SchedulerTuner{
		diskInfoProvider: diskInfoProvider,
		directories:      directories,
		devices:          devices,
		ioSchedulers:     []string{"none", "noop"},
		noMerges:         2,
		fs:               fs,
	}
}

func (tuner *SchedulerTuner) Tune() tuners.TuneResult {
	directoryDevices, err := tuner.diskInfoProvider.GetDirectoriesDevices(
		tuner.directories)
	if err != nil {
		return tuners.NewTuneError(err)
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
	disks := utils.GetKeys(disksSetMap)
	log.Infof("Tuning Schedulers of '%s' disks", disks)
	tuner.tuneDisks(disks)
	return tuners.NewTuneResult(false)
}

func (tuner *SchedulerTuner) CheckIfSupported() (
	supported bool,
	reason string,
) {
	if len(tuner.directories) == 0 && len(tuner.directories) == 0 {
		return false,
			"directories & devices are required for Disks Scheduler Tuner"
	}
	return true, ""
}

func (tuner *SchedulerTuner) tuneDisks(disks []string) {
	for _, disk := range disks {
		tuner.tuneDisk(disk)
	}
}

func (tuner *SchedulerTuner) tuneDisk(device string) {
	deviceNode := fmt.Sprintf("/dev/%s", device)
	ioScheduler := tuner.getIoScheduler(deviceNode)
	if ioScheduler == "" {
		log.Infof("Not setting I/O Scheduler for '%s' "+
			"- required schedulers '%s' are not supported",
			device, tuner.ioSchedulers)
	} else if !tuner.tuneIOScheduler(deviceNode, ioScheduler) {
		log.Infof("Not setting I/O Scheduler for %s -"+
			" feature not present", device)
	}

	if !tuner.tuneNoMerges(deviceNode) {
		log.Infof("Not setting 'nomerges' for '%s' -"+
			" feature not present", device)
	}
}

func (tuner *SchedulerTuner) tuneOneFeature(
	deviceNode string, pathCreator func(string) string, value string,
) bool {
	featureFile, _ := tuner.getFeatureFile(deviceNode, pathCreator)

	if featureFile == "" {
		return false
	}
	pathString := pathCreator(featureFile)
	log.Debugf("Setting '%s' with value '%s'", pathString, value)
	err := ioutil.WriteFile(pathString, []byte(value), 0644)
	if err != nil {
		log.Debugf("Unable to set '%s' in '%s' feature file",
			value, pathString)
		return false
	}
	return true
}

func (tuner *SchedulerTuner) tuneIOScheduler(
	deviceNode string, scheduler string,
) bool {
	return tuner.tuneOneFeature(deviceNode,
		func(pathStr string) string {
			return path.Join(pathStr, "queue", "scheduler")
		},
		scheduler)
}

func (tuner *SchedulerTuner) tuneNoMerges(deviceNode string) bool {
	return tuner.tuneOneFeature(deviceNode,
		func(pathStr string) string {
			return path.Join(pathStr, "queue", "nomerges")
		},
		strconv.Itoa(tuner.noMerges))
}

func (tuner *SchedulerTuner) getFeatureFile(
	deviceNode string, pathCreator func(string) string,
) (string, BlockDevice) {
	device, err := tuner.diskInfoProvider.GetBlockDeviceFromPath(deviceNode)
	if err != nil {
		return "", nil
	}
	featureFile := pathCreator(device.Syspath())
	if utils.FileExists(tuner.fs, featureFile) {
		return featureFile, device
	} else if device.Parent() != nil {
		return tuner.getFeatureFile(device.Parent().Devnode(), pathCreator)
	} else {
		return "", nil
	}
}

func (tuner *SchedulerTuner) getIoScheduler(deviceNode string) string {
	log.Debugf("Getting IO Scheduler for %s", deviceNode)
	featureFile, _ := tuner.getFeatureFile(deviceNode,
		func(pathStr string) string { return path.Join(pathStr, "queue", "scheduler") })
	log.Debugf("Feature file %s", featureFile)
	if featureFile == "" {
		return ""
	}
	lines, err := utils.ReadFileLines(tuner.fs, featureFile)
	if err != nil {
		log.Debugf("Unable to read feature file %s - IO schedulers feature"+
			" not available", featureFile)
		return ""
	}
	log.Debugf("Feature lines %s", lines)
	supportedSchedulers := map[string]bool{}
	for _, schedulerString := range strings.Split(lines[0], " ") {
		scheduler := strings.TrimRight(
			strings.TrimLeft(schedulerString, "["), "]")
		supportedSchedulers[scheduler] = true
	}
	for _, scheduler := range tuner.ioSchedulers {
		if supportedSchedulers[scheduler] {
			return scheduler
		}
	}

	return ""
}
