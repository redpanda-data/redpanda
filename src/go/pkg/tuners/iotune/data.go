package iotune

import (
	"fmt"

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

func DataFor(mountPoint, vendor, vm, storage string) (*IoProperties, error) {
	data := precompiledData()
	vms, ok := data[vendor]
	if !ok {
		return nil, fmt.Errorf("no iotune data found for vendor '%s'", vendor)
	}
	storages, ok := vms[vm]
	if !ok {
		return nil, fmt.Errorf("no iotune data found for VM '%s', of vendor '%s'", vm, vendor)
	}
	settings, ok := storages[storage]
	if !ok {
		return nil, fmt.Errorf("no iotune data found for storage '%s' in VM '%s', of vendor '%s'", storage, vm, vendor)
	}
	settings.MountPoint = mountPoint
	return &settings, nil
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
		"aws": map[string]map[string]io{
			"i3.large": map[string]io{
				"default": io{"", 111000, 653925080, 36800, 215066473},
			},
			"i3.xlarge": map[string]io{
				"default": io{"", 200800, 1185106376, 53180, 423621267},
			},
			"i3.2xlarge": map[string]io{
				"default": io{"", 411200, 2015342735, 181500, 808775652},
			},
			"i3.4xlarge": map[string]io{
				"default": io{"", 411200 * 2, 2015342735 * 2, 181500 * 2, 808775652 * 2},
			},
			"i3.8xlarge": map[string]io{
				"default": io{"", 411200 * 4, 2015342735 * 4, 181500 * 4, 808775652 * 4},
			},
			"i3.16xlarge": map[string]io{
				"default": io{"", 411200 * 8, 2015342735 * 8, 181500 * 8, 808775652 * 8},
			},
			"i3.metal": map[string]io{
				"default": io{"", 411200 * 8, 2015342735 * 8, 181500 * 8, 808775652 * 8},
			},
			"i3en.large": map[string]io{
				"default": io{"", 43315, 330301440, 33177, 165675008},
			},
			"i3en.xlarge": map[string]io{
				"default": io{"", 84480, 666894336, 66969, 333447168},
			},
			"i3en.2xlarge": map[string]io{
				"default": io{"", 84480 * 2, 666894336 * 2, 66969 * 2, 333447168 * 2},
			},
			"i3en.3xlarge": map[string]io{
				"default": io{"", 257024, 2043674624, 174080, 1024458752},
			},
			"i3en.6xlarge": map[string]io{
				"default": io{"", 257024 * 2, 2043674624 * 2, 174080 * 2, 1024458752 * 2},
			},
			"i3en.12xlarge": map[string]io{
				"default": io{"", 257024 * 4, 2043674624 * 4, 174080 * 4, 1024458752 * 4},
			},
			"i3en.24xlarge": map[string]io{
				"default": io{"", 257024 * 8, 2043674624 * 8, 174080 * 8, 1024458752 * 8},
			},
			"i3en.metal": map[string]io{
				"default": io{"", 257024 * 8, 2043674624 * 8, 174080 * 8, 1024458752 * 8},
			},
		},
	}
}
