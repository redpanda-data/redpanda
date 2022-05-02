// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package iotune

import (
	"fmt"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cloud/vendor"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
)

type IoProperties struct {
	MountPoint     string `yaml:"mountpoint"`
	ReadIops       int64  `yaml:"read_iops"`
	ReadBandwidth  int64  `yaml:"read_bandwidth"`
	WriteIops      int64  `yaml:"write_iops"`
	WriteBandwidth int64  `yaml:"write_bandwidth"`
}

type io = IoProperties

func DataFor(mountPoint, v, vm, storage string) (*IoProperties, error) {
	data := precompiledData()
	vms, ok := data[v]
	if !ok {
		return nil, fmt.Errorf("no iotune data found for vendor '%s'", v)
	}
	storages, ok := vms[vm]
	if !ok {
		return nil, fmt.Errorf("no iotune data found for VM '%s', of vendor '%s'", vm, v)
	}
	settings, ok := storages[storage]
	if !ok {
		return nil, fmt.Errorf("no iotune data found for storage '%s' in VM '%s', of vendor '%s'", storage, vm, v)
	}
	settings.MountPoint = mountPoint
	return &settings, nil
}

func DataForVendor(
	mountpoint string, v vendor.InitializedVendor,
) (*IoProperties, error) {
	vmType, err := v.VMType()
	if err != nil {
		return nil, fmt.Errorf("Couldn't get the current VM type for vendor '%s'", v.Name())
	}
	log.Infof("Detected vendor '%s' and VM type '%s'", v.Name(), vmType)
	return DataFor(mountpoint, v.Name(), vmType, "default")
}

func ToYaml(props IoProperties) (string, error) {
	type ioPropertiesWrapper struct {
		Disks []IoProperties `yaml:"disks"`
	}
	yaml, err := yaml.Marshal(ioPropertiesWrapper{[]io{props}})
	if err != nil {
		return "", err
	}
	return string(yaml), nil
}

func precompiledData() map[string]map[string]map[string]io {
	return map[string]map[string]map[string]io{
		"aws": {
			"i3.large": {
				"default": {"", 111000, 653925080, 36800, 215066473},
			},
			"i3.xlarge": {
				"default": {"", 200800, 1185106376, 53180, 423621267},
			},
			"i3.2xlarge": {
				"default": {"", 411200, 2015342735, 181500, 808775652},
			},
			"i3.4xlarge": {
				"default": {"", 411200 * 2, 2015342735 * 2, 181500 * 2, 808775652 * 2},
			},
			"i3.8xlarge": {
				"default": {"", 411200 * 4, 2015342735 * 4, 181500 * 4, 808775652 * 4},
			},
			"i3.16xlarge": {
				"default": {"", 411200 * 8, 2015342735 * 8, 181500 * 8, 808775652 * 8},
			},
			"i3.metal": {
				"default": {"", 411200 * 8, 2015342735 * 8, 181500 * 8, 808775652 * 8},
			},
			"i3en.large": {
				"default": {"", 43315, 330301440, 33177, 165675008},
			},
			"i3en.xlarge": {
				"default": {"", 84480, 666894336, 66969, 333447168},
			},
			"i3en.2xlarge": {
				"default": {"", 84480 * 2, 666894336 * 2, 66969 * 2, 333447168 * 2},
			},
			"i3en.3xlarge": {
				"default": {"", 257024, 2043674624, 174080, 1024458752},
			},
			"i3en.6xlarge": {
				"default": {"", 257024 * 2, 2043674624 * 2, 174080 * 2, 1024458752 * 2},
			},
			"i3en.12xlarge": {
				"default": {"", 257024 * 4, 2043674624 * 4, 174080 * 4, 1024458752 * 4},
			},
			"i3en.24xlarge": {
				"default": {"", 257024 * 8, 2043674624 * 8, 174080 * 8, 1024458752 * 8},
			},
			"i3en.metal": {
				"default": {"", 257024 * 8, 2043674624 * 8, 174080 * 8, 1024458752 * 8},
			},
		},
	}
}
