// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package disk

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/system"
	"github.com/spf13/afero"
	"go.uber.org/zap"
)

const (
	CachePolicyWriteThrough string = "write through"
	CachePolicyWriteBack    string = "write back"
)

type DeviceFeatures interface {
	GetScheduler(device string) (string, error)
	GetSupportedSchedulers(device string) ([]string, error)
	GetNomerges(device string) (int, error)
	GetNomergesFeatureFile(device string) (string, error)
	GetSchedulerFeatureFile(device string) (string, error)
	GetWriteCache(device string) (string, error)
	GetWriteCacheFeatureFile(device string) (string, error)
}

func NewDeviceFeatures(fs afero.Fs, blockDevices BlockDevices) DeviceFeatures {
	return &deviceFeatures{
		fs:           fs,
		blockDevices: blockDevices,
	}
}

type deviceFeatures struct {
	fs           afero.Fs
	blockDevices BlockDevices
}

func (d *deviceFeatures) GetScheduler(device string) (string, error) {
	schedulerOpts, err := d.getSchedulerOptions(device)
	if err != nil {
		return "", err
	}
	return schedulerOpts.GetActive(), nil
}

func (d *deviceFeatures) GetSupportedSchedulers(
	device string,
) ([]string, error) {
	schedulerOpts, err := d.getSchedulerOptions(device)
	if err != nil {
		return nil, err
	}
	return schedulerOpts.GetAvailable(), nil
}

func (d *deviceFeatures) GetNomerges(device string) (int, error) {
	zap.L().Sugar().Debugf("Getting '%s' nomerges", device)
	featureFile, err := d.GetNomergesFeatureFile(device)
	if err != nil {
		return 0, err
	}
	zap.L().Sugar().Debugf("Feature file %s", featureFile)
	bytes, err := afero.ReadFile(d.fs, featureFile)
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(strings.TrimSpace(string(bytes)))
}

func (d *deviceFeatures) GetNomergesFeatureFile(device string) (string, error) {
	return d.getQueueFeatureFile(deviceNode(device), "nomerges")
}

func (d *deviceFeatures) GetSchedulerFeatureFile(
	device string,
) (string, error) {
	return d.getQueueFeatureFile(deviceNode(device), "scheduler")
}

func (d *deviceFeatures) GetWriteCache(device string) (string, error) {
	zap.L().Sugar().Debugf("Getting '%s' write cache", device)
	featureFile, err := d.GetWriteCacheFeatureFile(device)
	if err != nil {
		return "", err
	}
	zap.L().Sugar().Debugf("Feature file %s", featureFile)
	bytes, err := afero.ReadFile(d.fs, featureFile)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(bytes)), nil
}

func (d *deviceFeatures) GetWriteCacheFeatureFile(
	device string,
) (string, error) {
	return d.getQueueFeatureFile(deviceNode(device), "write_cache")
}

func (d *deviceFeatures) getSchedulerOptions(
	device string,
) (*system.RuntimeOptions, error) {
	zap.L().Sugar().Debugf("Getting '%s' scheduler options", device)
	featureFile, err := d.GetSchedulerFeatureFile(device)
	if err != nil {
		return nil, err
	}
	zap.L().Sugar().Debugf("Feature file %s", featureFile)
	return system.ReadRuntineOptions(d.fs, featureFile)
}

func (d *deviceFeatures) getQueueFeatureFile(
	deviceNode string, featureType string,
) (string, error) {
	device, err := d.blockDevices.GetDeviceFromPath(deviceNode)
	if err != nil {
		zap.L().Sugar().Error(err.Error())
		return "", nil
	}
	featureFile := filepath.Join(device.Syspath(), "queue", featureType)
	zap.L().Sugar().Debugf("Trying to open feature file '%s'", featureFile)
	if exists, _ := afero.Exists(d.fs, featureFile); exists {
		return featureFile, nil
	} else if device.Parent() != nil {
		return d.getQueueFeatureFile(device.Parent().Devnode(), featureType)
	} else {
		return "", nil
	}
}

func deviceNode(deviceName string) string {
	return fmt.Sprintf("/dev/%s", deviceName)
}
