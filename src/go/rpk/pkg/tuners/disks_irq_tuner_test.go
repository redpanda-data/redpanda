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
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/disk"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/irq"
	"github.com/stretchr/testify/require"
)

type cpuMasksMock struct {
	irq.CPUMasks
	baseCPUMask              func(string) (string, error)
	cpuMaskForIRQs           func(irq.Mode, string) (string, error)
	getIRQsDistributionMasks func([]int, string) (map[int]string, error)
}

type blockDevicesMock struct {
	getDirectoriesDevices    func([]string) (map[string][]string, error)
	getDirectoryDevices      func(string) ([]string, error)
	getBlockDeviceFromPath   func(string) (disk.BlockDevice, error)
	getBlockDeviceSystemPath func(string) (string, error)
	getDiskInfoByType        func([]string) (map[disk.DiskType]disk.DevicesIRQs, error)
}

func (m *cpuMasksMock) BaseCPUMask(cpuMask string) (string, error) {
	return m.baseCPUMask(cpuMask)
}

func (m *cpuMasksMock) CPUMaskForIRQs(
	mode irq.Mode, cpuMask string,
) (string, error) {
	return m.cpuMaskForIRQs(mode, cpuMask)
}

func (m *cpuMasksMock) GetIRQsDistributionMasks(
	IRQs []int, cpuMask string,
) (map[int]string, error) {
	return m.getIRQsDistributionMasks(IRQs, cpuMask)
}

func (m *blockDevicesMock) GetDirectoriesDevices(
	directories []string,
) (map[string][]string, error) {
	return m.getDirectoriesDevices(directories)
}

func (m *blockDevicesMock) GetDeviceFromPath(
	path string,
) (disk.BlockDevice, error) {
	return m.getBlockDeviceFromPath(path)
}

func (m *blockDevicesMock) GetDeviceSystemPath(path string) (string, error) {
	return m.getBlockDeviceSystemPath(path)
}

func (m *blockDevicesMock) GetDirectoryDevices(path string) ([]string, error) {
	return m.getDirectoryDevices(path)
}

func (m *blockDevicesMock) GetDiskInfoByType(
	devices []string,
) (map[disk.DiskType]disk.DevicesIRQs, error) {
	return m.getDiskInfoByType(devices)
}

func TestGetExpectedIRQsDistribution(t *testing.T) {
	type args struct {
		devices      []string
		mode         irq.Mode
		cpuMask      string
		blockDevices disk.BlockDevices
		cpuMasks     irq.CPUMasks
	}
	tests := []struct {
		name    string
		args    args
		want    map[int]string
		wantErr bool
	}{
		{
			name: "shall return correct distribution",
			args: args{
				devices: []string{"dev1", "dev2"},
				mode:    irq.Sq,
				cpuMask: "0xff",
				blockDevices: &blockDevicesMock{
					getDiskInfoByType: func([]string) (map[disk.DiskType]disk.DevicesIRQs, error) {
						return map[disk.DiskType]disk.DevicesIRQs{
							disk.NonNvme: {
								Devices: []string{"dev1"},
								Irqs:    []int{10},
							},
							disk.Nvme: {
								Devices: []string{"dev1"},
								Irqs:    []int{12, 15, 18, 24},
							},
						}, nil
					},
				},
				cpuMasks: &cpuMasksMock{
					baseCPUMask: func(string) (string, error) {
						return "0x0000000f", nil
					},
					cpuMaskForIRQs: func(mode irq.Mode, cpuMask string) (string, error) {
						return "0x00000001", nil
					},
					getIRQsDistributionMasks: func(IRQs []int, cpuMask string) (map[int]string, error) {
						if cpuMask == "0x00000001" {
							return map[int]string{
								10: "0x00000001",
							}, nil
						}
						return map[int]string{
							12: "0x00000001",
							15: "0x00000002",
							18: "0x00000004",
							24: "0x00000008",
						}, nil
					},
				},
			},
			want: map[int]string{
				10: "0x00000001",
				12: "0x00000001",
				15: "0x00000002",
				18: "0x00000004",
				24: "0x00000008",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GetExpectedIRQsDistribution(
				tt.args.devices, tt.args.blockDevices,
				tt.args.mode, tt.args.cpuMask, tt.args.cpuMasks)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Exactly(t, tt.want, got)
		})
	}
}
