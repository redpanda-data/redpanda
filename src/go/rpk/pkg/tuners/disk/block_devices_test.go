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
	"strings"
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/os"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/irq"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

type irqDeviceInfoMock struct {
	irq.DeviceInfo
}

type irqProcFileMock struct {
	irq.ProcFile
	getIRQProcFileLinesMap func() (map[int]string, error)
}

func (m *irqProcFileMock) GetIRQProcFileLinesMap() (map[int]string, error) {
	return m.getIRQProcFileLinesMap()
}

type procMock struct {
	os.Proc
}

func Test_blockDevices_getDeviceControllerPath(t *testing.T) {
	// given
	fs := afero.NewMemMapFs()
	irqDeviceInfo := &irqDeviceInfoMock{}
	irqProcFile := &irqProcFileMock{}
	proc := &procMock{}
	blockDevices := &blockDevices{
		fs:            fs,
		irqDeviceInfo: irqDeviceInfo,
		irqProcFile:   irqProcFile,
		proc:          proc,
	}
	devSystemPath := "/sys/devices/pci0000:00/0000:00:1f.2/ata1/host0" +
		"/target0:0:0/0:0:0:0/block/sda/sda1"
	// when
	controllerPath, err := blockDevices.getDeviceControllerPath(devSystemPath)
	// then
	require.Nil(t, err)
	require.Equal(t, "/sys/devices/pci0000:00/0000:00:1f.2", controllerPath)

	// given
	var sb strings.Builder
	sb.WriteString("/sys/devices/LNXSYSTM:00/LNXSYBUS:00/PNP0A03:00/device:07/VMBUS:01")
	sb.WriteString("/129e938b-5639-4ac2-819d-5c2c778b0c49/pci5639:00/5639:00:00.0")
	expected := sb.String()
	sb.WriteString("/nvme/nvme0/nvme0n1")
	devSystemPath = sb.String()
	// when
	controllerPath, err = blockDevices.getDeviceControllerPath(devSystemPath)
	// then
	require.Nil(t, err)
	require.Equal(t, expected, controllerPath)
}

func Test_blockDevices_isIRQNvmeFastPathIRQ(t *testing.T) {
	// given
	fields := []struct {
		name     string
		procFile irq.ProcFile
		expected bool
		numCpus  int
	}{
		{
			name: "Shall return true as device with IRQ 18 is a NVMe device",
			procFile: &irqProcFileMock{
				getIRQProcFileLinesMap: func() (map[int]string, error) {
					procFileLine := "18:          0          0          0         " +
						" 0          0          0          0         21 " +
						"IR-PCI-MSI 59244544-edge      nvme0q4"
					return map[int]string{18: procFileLine}, nil
				},
			},
			expected: true,
			numCpus:  8,
		},
		{
			name: "Shall return false as device with IRQ 18 is a NVMe device" +
				"but queue number is larger than number of cpus",
			procFile: &irqProcFileMock{
				getIRQProcFileLinesMap: func() (map[int]string, error) {
					procFileLine := "18:          0          0          0         " +
						" 0          0          0          0         21 " +
						"IR-PCI-MSI 59244544-edge      nvme0q5"
					return map[int]string{18: procFileLine}, nil
				},
			},
			expected: false,
			numCpus:  4,
		},
		{
			name: "Shall return false as device with IRQ 18 is not NVMe device",
			procFile: &irqProcFileMock{
				getIRQProcFileLinesMap: func() (map[int]string, error) {
					procFileLine := "18:       1178       1469       3467" +
						"         96         17       3453       5932" +
						"        331  IR-PCI-MSI 333825-edge      iwlwifi: queue 1"
					return map[int]string{18: procFileLine}, nil
				},
			},
			expected: false,
			numCpus:  8,
		},
	}
	for _, test := range fields {
		t.Run(test.name, func(t *testing.T) {
			// given
			fs := afero.NewMemMapFs()
			irqDeviceInfo := &irqDeviceInfoMock{}
			proc := &procMock{}
			blockDevices := &blockDevices{
				fs:            fs,
				irqDeviceInfo: irqDeviceInfo,
				irqProcFile:   test.procFile,
				proc:          proc,
			}
			// when
			isNvmeIRQ, err := blockDevices.isIRQNvmeFastPathIRQ(18, test.numCpus)
			// then
			require.Nil(t, err)
			require.Equal(t, test.expected, isNvmeIRQ)
		})
	}
}
