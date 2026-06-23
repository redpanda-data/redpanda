// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

//go:build linux

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
	Duplex         bool   `json:"duplex"`
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
		return nil, fmt.Errorf("couldn't get the current VM type for provider '%s'", v.Name())
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
				"default": {"", 111000, 653925080, 36800, 215066473, false},
			},
			"i3.xlarge": {
				"default": {"", 200800, 1185106376, 53180, 423621267, false},
			},
			"i3.2xlarge": {
				"default": {"", 411200, 2015342735, 181500, 808775652, false},
			},
			"i3.4xlarge": {
				"default": {"", 411200 * 2, 2015342735 * 2, 181500 * 2, 808775652 * 2, false},
			},
			"i3.8xlarge": {
				"default": {"", 411200 * 4, 2015342735 * 4, 181500 * 4, 808775652 * 4, false},
			},
			"i3.16xlarge": {
				"default": {"", 411200 * 8, 2015342735 * 8, 181500 * 8, 808775652 * 8, false},
			},
			"i3.metal": {
				"default": {"", 411200 * 8, 2015342735 * 8, 181500 * 8, 808775652 * 8, false},
			},
			"i3en.large": {
				"default": {"", 43315, 330301440, 33177, 165675008, false},
			},
			"i3en.xlarge": {
				"default": {"", 84480, 666894336, 66969, 333447168, false},
			},
			"i3en.2xlarge": {
				"default": {"", 84480 * 2, 666894336 * 2, 66969 * 2, 333447168 * 2, false},
			},
			"i3en.3xlarge": {
				"default": {"", 257024, 2043674624, 174080, 1024458752, false},
			},
			"i3en.6xlarge": {
				"default": {"", 257024 * 2, 2043674624 * 2, 174080 * 2, 1024458752 * 2, false},
			},
			"i3en.12xlarge": {
				"default": {"", 257024 * 4, 2043674624 * 4, 174080 * 4, 1024458752 * 4, false},
			},
			"i3en.24xlarge": {
				"default": {"", 257024 * 8, 2043674624 * 8, 174080 * 8, 1024458752 * 8, false},
			},
			"i3en.metal": {
				"default": {"", 257024 * 8, 2043674624 * 8, 174080 * 8, 1024458752 * 8, false},
			},

			// is4gen values are taken from measurements except where noted.
			// For reference, the advertised IOPS values also also shown, these are from:
			// https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/storage-optimized-instances.html
			// Except for is4gen.8xlarge the alignment between advertised and measured IOPS is very close.
			// Test results on is4gen show that the BW/IOPS ratio is 8K for reads and 5K for writes.
			"is4gen.medium": {
				// advertised   31250             25000
				"default": {"", 31407, 251665056, 25105, 125274040, false},
			},

			"is4gen.large": {
				// advertised   62500             50000
				"default": {"", 62804, 503442048, 50214, 251087344, false},
			},

			"is4gen.xlarge": {
				// advertised   125000              100000
				"default": {"", 125659, 1015560064, 100404, 502173344, false},
			},

			"is4gen.2xlarge": {
				// advertised   250000              200000
				"default": {"", 251076, 2066053248, 201105, 1012893056, false},
			},

			"is4gen.4xlarge": {
				// advertised   500000              400000
				"default": {"", 502117, 4132124160, 402148, 2025763456, false},
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
				"default": {"", 2 * 502117, 2 * 4132124160, 2 * 402148, 2 * 2025763456, false},
			},

			"im4gn.large": {
				// advertised   31250             25000
				"default": {"", 31407, 251664976, 25105, 125273408, false},
			},

			"im4gn.xlarge": {
				// advertised   62500             50000
				"default": {"", 62809, 503439040, 50214, 251087600, false},
			},

			"im4gn.2xlarge": {
				// advertised   125000              100000
				"default": {"", 125664, 1015557248, 100411, 502192960, false},
			},

			"im4gn.4xlarge": {
				// advertised   250000              200000
				"default": {"", 251063, 2066157056, 201088, 1012886080, false},
			},

			"im4gn.8xlarge": {
				// advertised   500000              400000
				"default": {"", 502106, 4132167424, 402129, 2025818624, false},
			},

			// see comment on is4gen.8xlarge for what's going on here
			"im4gn.16xlarge": {
				// advertised      1000000                      800000
				// measured         556572,     8128993280,     497509,    4051727616},
				"default": {"", 2 * 502106, 2 * 4132167424, 2 * 402129, 2 * 2025818624, false},
			},

			// i4i values are taken from direct measurement with iotune except
			// where noted.
			// i4i has a ~7K read BW/IOPS ratio and a 10K write BW/IOPS ratio,
			// where the write side is notably higher than other storage optimized
			// instance types.
			"i4i.large": {
				// advertised:  50000             27500
				"default": {"", 50203, 352041984, 27599, 275442496, false},
			},

			"i4i.xlarge": {
				// advertised:  100000             55000
				"default": {"", 100371, 707373312, 55269, 552871552, false},
			},

			"i4i.2xlarge": {
				// advertised:  200000              110000
				"default": {"", 200633, 1427463808, 110542, 1113520256, false},
			},

			// at 4xlarge and above the read-side IOPS values start to scale
			// more poorly that advertised/expected, so use scaled values
			// from 2xlarge instead, under the assumption it is a measurement
			// limitation
			"i4i.4xlarge": {
				// advertised:      400000                  220000
				// measured:        302635,     2853325312, 221201, 2259740160,
				"default": {"", 2 * 200633, 2 * 1427463808, 2 * 110542, 2 * 1113520256, false},
			},

			"i4i.8xlarge": {
				// advertised:      800000                      440000
				// measured         519944,     5704762368,     442395,     4519457792},
				"default": {"", 4 * 200633, 4 * 1427463808, 4 * 110542, 4 * 1113520256, false},
			},

			// 12x large was not measured
			"i4i.12xlarge": {
				// advertised:     1200000                      660000
				"default": {"", 6 * 200633, 6 * 1427463808, 6 * 110542, 6 * 1113520256, false},
			},

			"i4i.16xlarge": {
				// advertised:      16000000                    880000
				// measured         752697,     8585068032,     750060,     8211869696},
				"default": {"", 8 * 200633, 8 * 1427463808, 8 * 110542, 8 * 1113520256, false},
			},

			// 24x large was not measured
			"i4i.24xlarge": {
				// advertised:      2400000                       1320000
				"default": {"", 12 * 200633, 12 * 1427463808, 12 * 110542, 12 * 1113520256, false},
			},

			// 32x large was not measured, iotune fails to start
			// see https://github.com/redpanda-data/redpanda/issues/17154
			"i4i.32xlarge": {
				// advertised:      3200000                       1760000
				"default": {"", 16 * 200633, 16 * 1427463808, 16 * 110542, 16 * 1113520256, false},
			},

			// m7gd values are taken from direct measurement with iotune except
			// where noted.
			"m7gd.large": {
				// advertised:  33542             16771
				"default": {"", 33638, 157521648, 16836, 75264032, true},
			},

			"m7gd.xlarge": {
				// advertised:  67084             33542
				"default": {"", 67274, 315774176, 33676, 150679296, true},
			},

			"m7gd.2xlarge": {
				// advertised:  134168             67084
				"default": {"", 134550, 631589440, 67358, 301293568, true},
			},

			"m7gd.4xlarge": {
				// advertised:  268336              134168
				"default": {"", 269588, 1263357056, 134608, 604995968, true},
			},

			// These numbers were measured on earlier versions of iotune which
			// suffered from a bug where on larger instance sizes the
			// read-side IOPS values start to scale more poorly than
			// advertised/expected. So we use scaled values from 4xlarge
			// instead. This is fixed in the latest iotune version though it
			// still requires running with explicit extremely high io-depth.
			//
			// Further these disks aren't 100% full-duplex (more like 50% full
			// duplex or so). Nevertheless we still mark them as full duplex.
			// Otherwise we let too much perf go to waste compared to running
			// without io-properties.
			"m7gd.8xlarge": {
				// advertised:      536672                      268336
				"default": {"", 2 * 269588, 2 * 1263357056, 2 * 134608, 2 * 604995968, true},
			},

			"m7gd.12xlarge": {
				// advertised:      805008                      402504
				"default": {"", 3 * 269588, 3 * 1263357056, 3 * 134608, 3 * 604995968, true},
			},

			"m7gd.16xlarge": {
				// advertised:     1073344                      536672
				"default": {"", 4 * 269588, 4 * 1263357056, 4 * 134608, 4 * 604995968, true},
			},

			// m8gd values are taken from direct measurement with iotune except
			// where noted.
			"m8gd.large": {
				"default": {"", 33638, 224 * 1024 * 1024, 16836, 109 * 1024 * 1024, true},
			},

			"m8gd.xlarge": {
				"default": {"", 67274, 450 * 1024 * 1024, 33676, 215 * 1024 * 1024, true},
			},

			"m8gd.2xlarge": {
				"default": {"", 134550, 904 * 1024 * 1024, 67358, 431 * 1024 * 1024, true},
			},

			"m8gd.4xlarge": {
				"default": {"", 269588, 1821 * 1024 * 1024, 134608, 865 * 1024 * 1024, true},
			},

			// These disks aren't 100% full-duplex (more like 50% full
			// duplex or so). Nevertheless we still mark them as full duplex.
			// Otherwise we let too much perf go to waste compared to running
			// without io-properties.
			"m8gd.8xlarge": {
				"default": {"", 541806, 3699 * 1024 * 1024, 268818, 1743 * 1024 * 1024, true},
			},

			// Read IOPS values are upscaled because default iotune struggles to
			// reach full RIOPS with default settings (needs either lowered smp
			// or increased iodepth) but I was too lazy to implement that but
			// have confirmed that we can reach those numbers otherwise.
			"m8gd.12xlarge": {
				"default": {"", 3 * 269588, 5465 * 1024 * 1024, 402765, 2596 * 1024 * 1024, true},
			},

			"m8gd.16xlarge": {
				"default": {"", 4 * 269588, 7398 * 1024 * 1024, 540197, 3590 * 1024 * 1024, true},
			},

			// m9gd values are taken from direct measurement with iotune except
			// where noted.
			"m9gd.large": {
				// advertised:  43604             21802
				"default": {"", 43614, 306225472, 21803, 146621200, true},
			},

			"m9gd.xlarge": {
				// advertised:  87207             43604
				"default": {"", 87229, 612447872, 43614, 293574688, true},
			},

			"m9gd.2xlarge": {
				// advertised:  174417              87209
				"default": {"", 174581, 1230804224, 87229, 587151104, true},
			},

			"m9gd.4xlarge": {
				// advertised:  348832              174417
				"default": {"", 349161, 2461534976, 174580, 1174274048, true},
			},

			"m9gd.8xlarge": {
				// advertised:  697665              348834
				"default": {"", 702226, 4923082752, 349161, 2370320640, true},
			},

			"m9gd.12xlarge": {
				// advertised:  1046496              523251
				"default": {"", 1047209, 7382385664, 523544, 3522541824, true},
			},

			// Read IOPS values are upscaled because default iotune struggles to
			// reach full RIOPS with default settings on the largest instance size.
			"m9gd.16xlarge": {
				// advertised:      1395331                 697668
				// measured         862638,          		698973,
				"default": {"", 2 * 702226, 2 * 4923082752, 698973, 4740612096, true},
			},

			// m6id values are copied from m7gd as they should have the same
			// disks and behave the same from tests.
			"m6id.large": {
				// advertised:  33542             16771
				"default": {"", 33638, 157521648, 16836, 75264032, true},
			},

			"m6id.xlarge": {
				// advertised:  67084             33542
				"default": {"", 67274, 315774176, 33676, 150679296, true},
			},

			"m6id.2xlarge": {
				// advertised:  134168             67084
				"default": {"", 134550, 631589440, 67358, 301293568, true},
			},

			"m6id.4xlarge": {
				// advertised:  268336              134168
				"default": {"", 269588, 1263357056, 134608, 604995968, true},
			},

			// see comments above for larger m7gd instance types
			"m6id.8xlarge": {
				// advertised:      536672                      268336
				"default": {"", 2 * 269588, 2 * 1263357056, 2 * 134608, 2 * 604995968, true},
			},

			"m6id.12xlarge": {
				// advertised:      805008                      402504
				"default": {"", 3 * 269588, 3 * 1263357056, 3 * 134608, 3 * 604995968, true},
			},

			"m6id.16xlarge": {
				// advertised:     1073344                      536672
				"default": {"", 4 * 269588, 4 * 1263357056, 4 * 134608, 4 * 604995968, true},
			},
		},
	}
}
