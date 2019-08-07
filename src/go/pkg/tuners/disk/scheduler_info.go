package disk

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"vectorized/pkg/system"
	"vectorized/pkg/utils"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
)

type SchedulerInfo interface {
	GetScheduler(device string) (string, error)
	GetSupportedSchedulers(device string) ([]string, error)
	GetNomerges(device string) (int, error)
	GetNomergesFeatureFile(device string) (string, error)
	GetSchedulerFeatureFile(device string) (string, error)
}

func NewSchedulerInfo(fs afero.Fs, blockDevices BlockDevices) SchedulerInfo {
	return &schedulerInfo{
		fs:           fs,
		blockDevices: blockDevices,
	}
}

type schedulerInfo struct {
	SchedulerInfo
	fs           afero.Fs
	blockDevices BlockDevices
}

func (s *schedulerInfo) GetScheduler(device string) (string, error) {
	schedulerOpts, err := s.getSchedulerOptions(device)
	if err != nil {
		return "", err
	}
	return schedulerOpts.GetActive(), nil
}

func (s *schedulerInfo) GetSupportedSchedulers(
	device string,
) ([]string, error) {
	schedulerOpts, err := s.getSchedulerOptions(device)
	if err != nil {
		return nil, err
	}
	return schedulerOpts.GetAvailable(), nil
}

func (s *schedulerInfo) GetNomerges(device string) (int, error) {
	log.Debugf("Getting '%s' nomerges", device)
	featureFile, err := s.GetNomergesFeatureFile(device)
	if err != nil {
		return 0, err
	}
	log.Debugf("Feature file %s", featureFile)
	bytes, err := afero.ReadFile(s.fs, featureFile)
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(strings.TrimSpace(string(bytes)))
}

func (s *schedulerInfo) GetNomergesFeatureFile(device string) (string, error) {
	return s.getQueueFeatureFile(deviceNode(device), "nomerges")
}
func (s *schedulerInfo) GetSchedulerFeatureFile(device string) (string, error) {
	return s.getQueueFeatureFile(deviceNode(device), "scheduler")
}

func (s *schedulerInfo) getSchedulerOptions(
	device string,
) (*system.RuntimeOptions, error) {
	log.Debugf("Getting '%s' scheduler options", device)
	featureFile, err := s.GetSchedulerFeatureFile(device)
	if err != nil {
		return nil, err
	}
	log.Debugf("Feature file %s", featureFile)
	return system.ReadRuntineOptions(s.fs, featureFile)
}

func (s *schedulerInfo) getQueueFeatureFile(
	deviceNode string, featureType string,
) (string, error) {
	device, err := s.blockDevices.GetDeviceFromPath(deviceNode)
	if err != nil {
		log.Error(err.Error())
		return "", nil
	}
	featureFile := filepath.Join(device.Syspath(), "queue", featureType)
	log.Debugf("Trying to open feature file '%s'", featureFile)
	if utils.FileExists(s.fs, featureFile) {
		return featureFile, nil
	} else if device.Parent() != nil {
		return s.getQueueFeatureFile(device.Parent().Devnode(), featureType)
	} else {
		return "", nil
	}
}

func deviceNode(deviceName string) string {
	return fmt.Sprintf("/dev/%s", deviceName)
}
