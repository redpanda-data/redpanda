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
	"encoding/json"
	"fmt"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cloud/provider"
)

type IoProperties struct {
	MountPoint     string `json:"mountpoint"`
	ReadIops       int64  `json:"read_iops"`
	ReadBandwidth  int64  `json:"read_bandwidth"`
	WriteIops      int64  `json:"write_iops"`
	WriteBandwidth int64  `json:"write_bandwidth"`
}

type io = IoProperties

func DataFor(mountPoint, v, vm, storage string) (*IoProperties, error) {
	data := precompiledData()
	vms, ok := data[v]
	if !ok {
		return nil, fmt.Errorf("no iotune data found for provider '%s'", v)
	}
	storages, ok := vms[vm]
	if !ok {
		return nil, fmt.Errorf("no iotune data found for VM '%s', of provider '%s'", vm, v)
	}
	settings, ok := storages[storage]
	if !ok {
		return nil, fmt.Errorf("no iotune data found for storage '%s' in VM '%s', of provider '%s'", storage, vm, v)
	}
	settings.MountPoint = mountPoint
	return &settings, nil
}

func DataForProvider(
	mountpoint string, v provider.InitializedProvider,
) (*IoProperties, error) {
	vmType, err := v.VMType()
	if err != nil {
		return nil, fmt.Errorf("Couldn't get the current VM type for provider '%s'", v.Name())
	}
	fmt.Printf("Detected provider '%s' and VM type '%s'\n", v.Name(), vmType)
	return DataFor(mountpoint, v.Name(), vmType, "default")
}

func ToJSON(props IoProperties) (string, error) {
	type ioPropertiesWrapper struct {
		Disks []IoProperties `json:"disks"`
	}
	json, err := json.Marshal(ioPropertiesWrapper{[]io{props}})
	if err != nil {
		return "", err
	}
	return string(json), nil
}

func precompiledData() map[string]map[string]map[string]io {
	// see IoProperties for field mapping
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

			// is4gen values are taken from measurements except where noted.
			// For reference, the advertised IOPS values also also shown, these are from:
			// https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/storage-optimized-instances.html
			// Except for is4gen.8xlarge the alignment between advertised and measured IOPS is very close.
			// Test results on is4gen show that the BW/IOPS ratio is 8K for reads and 5K for writes.
			"is4gen.medium": {
				// advertised   31250             25000
				"default": {"", 31407, 251665056, 25105, 125274040},
			},

			"is4gen.large": {
				// advertised   62500             50000
				"default": {"", 62804, 503442048, 50214, 251087344},
			},

			"is4gen.xlarge": {
				// advertised   125000              100000
				"default": {"", 125659, 1015560064, 100404, 502173344},
			},

			"is4gen.2xlarge": {
				// advertised   250000              200000
				"default": {"", 251076, 2066053248, 201105, 1012893056},
			},

			"is4gen.4xlarge": {
				// advertised   500000              400000
				"default": {"", 502117, 4132124160, 402148, 2025763456},
			},

			// The measured values for is4gen.8xlarge broke the expected pattern
			// and compared to 4xlarge for IOPS were slightly lower (!!) on the read side and
			// only slightly higher on the write side, rather than the expected 2x
			// jump. Signs point towards a measurement limitation rather than a real
			// limitation, so for now we use 2x the prior instance type values.
			// See: https://github.com/redpanda-data/redpanda/issues/17162.
			"is4gen.8xlarge": {
				// advertised      1000000                      800000
				// measured         457957,     7433367040,     433330,     4051469824
				"default": {"", 2 * 502117, 2 * 4132124160, 2 * 402148, 2 * 2025763456},
			},

			"im4gn.large": {
				// advertised   31250             25000
				"default": {"", 31407, 251664976, 25105, 125273408},
			},

			"im4gn.xlarge": {
				// advertised   62500             50000
				"default": {"", 62809, 503439040, 50214, 251087600},
			},

			"im4gn.2xlarge": {
				// advertised   125000              100000
				"default": {"", 125664, 1015557248, 100411, 502192960},
			},

			"im4gn.4xlarge": {
				// advertised   250000              200000
				"default": {"", 251063, 2066157056, 201088, 1012886080},
			},

			"im4gn.8xlarge": {
				// advertised   500000              400000
				"default": {"", 502106, 4132167424, 402129, 2025818624},
			},

			// see comment on is4gen.8xlarge for what's going on here
			"im4gn.16xlarge": {
				// advertised      1000000                      800000
				// measured         556572,     8128993280,     497509,    4051727616},
				"default": {"", 2 * 502106, 2 * 4132167424, 2 * 402129, 2 * 2025818624},
			},

			// i4i values are taken from direct measurement with iotune except
			// where noted.
			// i4i has a ~7K read BW/IOPS ratio and a 10K write BW/IOPS ratio,
			// where the write side is notably higher than other storage optimized
			// instance types.
			"i4i.large": {
				// advertised:  50000             27500
				"default": {"", 50203, 352041984, 27599, 275442496},
			},

			"i4i.xlarge": {
				// advertised:  100000             55000
				"default": {"", 100371, 707373312, 55269, 552871552},
			},

			"i4i.2xlarge": {
				// advertised:  200000              110000
				"default": {"", 200633, 1427463808, 110542, 1113520256},
			},

			// at 4xlarge and above the read-side IOPS values start to scale
			// more poorly that advertised/expected, so use scaled values
			// from 2xlarge instead, under the assumption it is a measurement
			// limitation
			"i4i.4xlarge": {
				// advertised:      400000                  220000
				// measured:        302635,     2853325312, 221201, 2259740160,
				"default": {"", 2 * 200633, 2 * 1427463808, 2 * 110542, 2 * 1113520256},
			},

			"i4i.8xlarge": {
				// advertised:      800000                      440000
				// measured         519944,     5704762368,     442395,     4519457792},
				"default": {"", 4 * 200633, 4 * 1427463808, 4 * 110542, 4 * 1113520256},
			},

			// 12x large was not measured
			"i4i.12xlarge": {
				// advertised:     1200000                      660000
				"default": {"", 6 * 200633, 6 * 1427463808, 6 * 110542, 6 * 1113520256},
			},

			"i4i.16xlarge": {
				// advertised:      16000000                    880000
				// measured         752697,     8585068032,     750060,     8211869696},
				"default": {"", 8 * 200633, 8 * 1427463808, 8 * 110542, 8 * 1113520256},
			},

			// 24x large was not measured
			"i4i.24xlarge": {
				// advertised:      2400000                       1320000
				"default": {"", 12 * 200633, 12 * 1427463808, 12 * 110542, 12 * 1113520256},
			},

			// 32x large was not measured, iotune fails to start
			// see https://github.com/redpanda-data/redpanda/issues/17154
			"i4i.32xlarge": {
				// advertised:      3200000                       1760000
				"default": {"", 16 * 200633, 16 * 1427463808, 16 * 110542, 16 * 1113520256},
			},
		},
	}
}
