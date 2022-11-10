// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package irq

import (
	"os"
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/utils"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

type mockProcFile struct {
	ProcFile
	getIRQProcFileLinesMap func() (map[int]string, error)
}

func (mockProcFile *mockProcFile) GetIRQProcFileLinesMap() (
	map[int]string,
	error,
) {
	return mockProcFile.getIRQProcFileLinesMap()
}

func Test_DeviceInfo_GetIRQs(t *testing.T) {
	tests := []struct {
		name          string
		procFile      ProcFile
		before        func(afero.Fs)
		irqConfigDir  string
		xenDeviceName string
		want          []int
	}{
		{
			name: "Shall return the IRQs when device is using MSI IRQs",
			before: func(fs afero.Fs) {
				_ = afero.WriteFile(fs, "/irq_config/dev1/msi_irqs/1", []byte{}, os.ModePerm)
				_ = afero.WriteFile(fs, "/irq_config/dev1/msi_irqs/2", []byte{}, os.ModePerm)
				_ = afero.WriteFile(fs, "/irq_config/dev1/msi_irqs/5", []byte{}, os.ModePerm)
				_ = afero.WriteFile(fs, "/irq_config/dev1/msi_irqs/8", []byte{}, os.ModePerm)
			},
			irqConfigDir: "/irq_config/dev1",
			want:         []int{1, 2, 5, 8},
		},
		{
			name: "Shall return the IRQs when device is using INT#x IRQs",
			before: func(fs afero.Fs) {
				_ = utils.WriteFileLines(fs, []string{"1", "2", "5", "8"},
					"/irq_config/dev1/irq")
			},
			irqConfigDir: "/irq_config/dev1",
			want:         []int{1, 2, 5, 8},
		},
		{
			name: "Shall return the IRQs from virtio device",
			procFile: &mockProcFile{
				getIRQProcFileLinesMap: func() (map[int]string, error) {
					return map[int]string{
						1: "1:     184233          0          0       7985   IO-APIC   1-edge      i8042",
						5: "5:          0          0          0          0   IO-APIC   5-edge      drv-virtio-1",
						8: "8:          1          0          0          0   IO-APIC   8-edge      rtc0",
					}, nil
				},
			},
			before: func(fs afero.Fs) {
				_ = utils.WriteFileLines(fs,
					[]string{"virtio:v00008086d000024DBsv0000103Csd0000006Abc01sc01i8A"},
					"/irq_config/dev1/modalias")
				_ = utils.WriteFileLines(fs,
					[]string{},
					"/irq_config/dev1/driver/drv-virtio-1")
			},
			irqConfigDir:  "/irq_config/dev1",
			xenDeviceName: "dev1",
			want:          []int{5},
		},
		{
			name: "Shall return the IRQs using XEN device name",
			procFile: &mockProcFile{
				getIRQProcFileLinesMap: func() (map[int]string, error) {
					return map[int]string{
						1: "1:     184233          0          0       7985   IO-APIC   1-edge      xen-dev1",
						5: "5:          0          0          0          0   IO-APIC   5-edge      drv-virtio-1",
						8: "8:          1          0          0          0   IO-APIC   8-edge      rtc0",
					}, nil
				},
			},
			before: func(fs afero.Fs) {
				_ = utils.WriteFileLines(fs,
					[]string{"xen:v00008086d000024DBsv0000103Csd0000006Abc01sc01i8A"},
					"/irq_config/dev1/modalias")
				_ = utils.WriteFileLines(fs,
					[]string{},
					"/irq_config/dev1/driver/drv-virtio-1")
			},
			irqConfigDir:  "/irq_config/dev1",
			xenDeviceName: "xen-dev1",
			want:          []int{1},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := afero.NewMemMapFs()
			tt.before(fs)
			deviceInfo := NewDeviceInfo(fs, tt.procFile)
			got, err := deviceInfo.GetIRQs(tt.irqConfigDir, tt.xenDeviceName)
			require.NoError(t, err)
			require.Exactly(t, tt.want, got)
		})
	}
}

func TestFindModalias(t *testing.T) {
	for _, tt := range []struct {
		name             string
		init             string
		modaliasFilePath string
		expErr           bool
	}{
		{"in block device", "/sys/devices/vbd-51792/block/xvdf/", "/sys/devices/vbd-51792/block/xvdf/modalias", false},
		{"in device", "/sys/devices/vbd-51792/block/xvdf/", "/sys/devices/vbd-51792/modalias", false},
		{"starting very deep", "/sys/devices/vbd-51792/block/xvdf/xvdf-1/xvdf-part/block/xvdf-a", "/sys/devices/vbd-51792/modalias", false},
		{"no modalias", "/sys/devices/vbd-51792/block/xvdf/", "", true},
		{"no modalias in unknown path", "/home/redpanda/opt/bin/device", "", true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fs := afero.NewMemMapFs()
			if tt.modaliasFilePath != "" {
				err := afero.WriteFile(fs, tt.modaliasFilePath, []byte{}, 0o777)
				require.NoError(t, err)
			}
			modalias, err := findModalias(fs, tt.init)
			if tt.expErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Exactly(t, tt.modaliasFilePath, modalias)
		})
	}
}
