// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package tuners

import (
	"fmt"
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cloud/vendor"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/executors"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

type vendorMock struct {
	init func() (vendor.InitializedVendor, error)
}

type currentVendor struct {
	name string
}

func (v *vendorMock) Init() (vendor.InitializedVendor, error) {
	return v.init()
}

func (*vendorMock) Name() string {
	return "none"
}

func (v *currentVendor) Name() string {
	return v.name
}

func (*currentVendor) VMType() (string, error) {
	return "", nil
}

const devicePath = "/sys/devices/pci0000:00/0000:00:1d.0/0000:71:00.0/nvme/fake"

func TestDeviceWriteCacheTuner_Tune(t *testing.T) {
	// given
	v := &vendorMock{
		init: func() (vendor.InitializedVendor, error) {
			return &currentVendor{
				name: "gcp",
			}, nil
		},
	}
	deviceFeatures := &deviceFeaturesMock{
		getWriteCacheFeatureFile: func(string) (string, error) {
			return devicePath + "/queue/write_cache", nil
		},
		getWriteCache: func(string) (string, error) {
			return "write back", nil
		},
	}
	fs := afero.NewMemMapFs()
	fs.MkdirAll(devicePath+"/queue", 0o644)
	tuner := NewDeviceGcpWriteCacheTuner(fs, "fake", deviceFeatures, v, executors.NewDirectExecutor())
	// when
	tuner.Tune()
	// then
	setValue, _ := afero.ReadFile(fs, devicePath+"/queue/write_cache")
	require.Equal(t, "write through", string(setValue))
}

func TestGCPCacheTunerSupported(t *testing.T) {
	type args struct {
		vendor vendor.Vendor
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "should be supported on GCP",
			args: args{
				vendor: &vendorMock{
					init: func() (vendor.InitializedVendor, error) {
						return &currentVendor{
							name: "gcp",
						}, nil
					},
				},
			},
			want: true,
		},
		{
			name: "should not be supported on AWS",
			args: args{
				vendor: &vendorMock{
					init: func() (vendor.InitializedVendor, error) {
						return &currentVendor{
							name: "aws",
						}, nil
					},
				},
			},
			want: false,
		},
		{
			name: "should not be supported on not cloud deployments",
			args: args{
				vendor: &vendorMock{
					init: func() (vendor.InitializedVendor, error) {
						return nil, fmt.Errorf("no vendor")
					},
				},
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tuner := NewDeviceGcpWriteCacheTuner(afero.NewMemMapFs(), "fake",
				&deviceFeaturesMock{}, tt.args.vendor,
				executors.NewDirectExecutor())
			supported, _ := tuner.CheckIfSupported()
			require.Equal(t, tt.want, supported)
		})
	}
}
