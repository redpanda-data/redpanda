// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package network

import (
	"fmt"
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/ethtool"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/irq"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

type procFileMock struct {
	irq.ProcFile
	getIRQProcFileLinesMap func() (map[int]string, error)
}

func (m *procFileMock) GetIRQProcFileLinesMap() (map[int]string, error) {
	return m.getIRQProcFileLinesMap()
}

type deviceInfoMock struct {
	irq.DeviceInfo
	getIRQs func(string, string) ([]int, error)
}

func (m *deviceInfoMock) GetIRQs(path string, device string) ([]int, error) {
	return m.getIRQs(path, device)
}

type ethtoolMock struct {
	ethtool.EthtoolWrapper
	driverName func(string) (string, error)
	features   func(string) (map[string]bool, error)
}

func (m *ethtoolMock) DriverName(iface string) (string, error) {
	return m.driverName(iface)
}

func (m *ethtoolMock) Features(iface string) (map[string]bool, error) {
	return m.features(iface)
}

func Test_nic_IsBondIface(t *testing.T) {
	// given
	fs := afero.NewMemMapFs()
	procFile := &procFileMock{}
	deviceInfo := &deviceInfoMock{}
	nic := NewNic(fs, procFile, deviceInfo, &ethtoolMock{}, "test0")
	afero.WriteFile(fs, "/sys/class/net/bond_masters", []byte(fmt.Sprintln("test0")), 0o644)
	// when
	bond := nic.IsBondIface()
	// then
	require.True(t, bond)
}

func Test_nic_Slaves_ReturnAllSlavesOfAnInterface(t *testing.T) {
	// given
	fs := afero.NewMemMapFs()
	procFile := &procFileMock{}
	deviceInfo := &deviceInfoMock{}
	nic := NewNic(fs, procFile, deviceInfo, &ethtoolMock{}, "test0")
	afero.WriteFile(fs, "/sys/class/net/bond_masters", []byte(fmt.Sprintln("test0")), 0o644)
	afero.WriteFile(fs, "/sys/class/net/test0/bond/slaves", []byte("sl0\nsl1\nsl2"), 0o644)
	// when
	slaves, err := nic.Slaves()
	// then
	require.NoError(t, err)
	require.Len(t, slaves, 3)
	require.Equal(t, slaves[0].Name(), "sl0")
	require.Equal(t, slaves[1].Name(), "sl1")
	require.Equal(t, slaves[2].Name(), "sl2")
}

func Test_nic_Slaves_ReturnEmptyForNotBondInterface(t *testing.T) {
	// given
	fs := afero.NewMemMapFs()
	procFile := &procFileMock{}
	deviceInfo := &deviceInfoMock{}
	nic := NewNic(fs, procFile, deviceInfo, &ethtoolMock{}, "test0")
	// when
	slaves, err := nic.Slaves()
	// then
	require.NoError(t, err)
	require.Empty(t, slaves)
}

func Test_nic_GetIRQs(t *testing.T) {
	tests := []struct {
		name          string
		irqProcFile   irq.ProcFile
		irqDeviceInfo irq.DeviceInfo
		ethtool       ethtool.EthtoolWrapper
		nicName       string
		want          []int
	}{
		{
			name: "Shall return all device IRQs when there are not fast paths",
			irqProcFile: &procFileMock{
				getIRQProcFileLinesMap: func() (map[int]string, error) {
					return map[int]string{
						54: "54:       9076       8545       3081       1372       4662     190816       3865       6709  IR-PCI-MSI 333825-edge      iwlwifi: queue 1",
						56: "56:      24300       3370        681       2725       1511       6627      21983       7056  IR-PCI-MSI 333826-edge      iwlwifi: queue 2",
						58: "58:       8444      10072       3025       2732       5432       5919       7217       3559  IR-PCI-MSI 333827-edge      iwlwifi: queue 3",
					}, nil
				},
			},
			irqDeviceInfo: &deviceInfoMock{
				getIRQs: func(string, string) ([]int, error) {
					return []int{54, 56, 58}, nil
				},
			},
			want: []int{54, 56, 58},
		},
		{
			name: "Shall return fast path IRQs only sorted by queue number",
			irqProcFile: &procFileMock{
				getIRQProcFileLinesMap: func() (map[int]string, error) {
					return map[int]string{
						91: "91:      40351          0          0          0   PCI-MSI 1572868-edge      eth0",
						92: "92:      79079          0          0          0   PCI-MSI 1572865-edge      eth0-TxRx-3",
						93: "93:      60344          0          0          0   PCI-MSI 1572866-edge      eth0-TxRx-2",
						94: "94:      48929          0          0          0   PCI-MSI 1572867-edge      eth0-TxRx-1",
						95: "95:      40351          0          0          0   PCI-MSI 1572868-edge      eth0-TxRx-0",
					}, nil
				},
			},
			irqDeviceInfo: &deviceInfoMock{
				getIRQs: func(string, string) ([]int, error) {
					return []int{91, 92, 93, 94, 95}, nil
				},
			},
			want: []int{95, 94, 93, 92},
		},
		{
			name: "Fdir fast path IRQs should be moved to the end of list",
			irqProcFile: &procFileMock{
				getIRQProcFileLinesMap: func() (map[int]string, error) {
					return map[int]string{
						91: "91:      40351          0          0          0   PCI-MSI 1572868-edge      eth0",
						92: "92:      79079          0          0          0   PCI-MSI 1572865-edge      eth0-TxRx-3",
						93: "93:      60344          0          0          0   PCI-MSI 1572866-edge      eth0-TxRx-2",
						94: "94:      48929          0          0          0   PCI-MSI 1572867-edge      eth0-TxRx-1",
						95: "95:      40351          0          0          0   PCI-MSI 1572868-edge      eth0-TxRx-0",
						96: "96:      40351          0          0          0   PCI-MSI 1572868-edge      eth0-fdir-TxRx-0",
					}, nil
				},
			},
			irqDeviceInfo: &deviceInfoMock{
				getIRQs: func(string, string) ([]int, error) {
					return []int{91, 92, 93, 94, 95, 96}, nil
				},
			},
			want: []int{95, 94, 93, 92, 96},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := &nic{
				fs:            afero.NewMemMapFs(),
				irqProcFile:   tt.irqProcFile,
				irqDeviceInfo: tt.irqDeviceInfo,
				ethtool:       &ethtoolMock{},
				name:          "test0",
			}
			got, err := n.GetIRQs()
			require.NoError(t, err)
			require.Exactly(t, tt.want, got)
		})
	}
}

func Test_nic_GetMaxRxQueueCount(t *testing.T) {
	tests := []struct {
		name    string
		ethtool ethtool.EthtoolWrapper
		want    int
	}{
		{
			name: "Shall return correct max queues for ixgbe driver",
			ethtool: &ethtoolMock{
				driverName: func(string) (string, error) {
					return "ixgbe", nil
				},
			},
			want: 16,
		},
		{
			name: "Shall return max int when driver is unknown",
			ethtool: &ethtoolMock{
				driverName: func(string) (string, error) {
					return "iwlwifi", nil
				},
			},
			want: MaxInt,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := &nic{
				fs:            afero.NewMemMapFs(),
				irqProcFile:   &procFileMock{},
				irqDeviceInfo: &deviceInfoMock{},
				ethtool:       tt.ethtool,
				name:          "test0",
			}
			got, err := n.GetMaxRxQueueCount()
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func Test_nic_GetRxQueueCount(t *testing.T) {
	tests := []struct {
		name          string
		irqProcFile   irq.ProcFile
		irqDeviceInfo irq.DeviceInfo
		ethtool       ethtool.EthtoolWrapper
		before        func(fs afero.Fs)
		want          int
	}{
		{
			name: "Shall return len(IRQ) when RPS is disabled and driver is not limiting queus number",
			irqProcFile: &procFileMock{
				getIRQProcFileLinesMap: func() (map[int]string, error) {
					return map[int]string{
						91: "91:...",
						92: "92:...",
						93: "93:...",
						94: "94:...",
						95: "95:...",
					}, nil
				},
			},
			irqDeviceInfo: &deviceInfoMock{
				getIRQs: func(string, string) ([]int, error) {
					return []int{91, 92, 93, 94, 95}, nil
				},
			},
			ethtool: &ethtoolMock{
				driverName: func(string) (string, error) {
					return "iwlwifi", nil
				},
			},
			before: func(fs afero.Fs) {
			},
			want: 5,
		},
		{
			name:          "Shall return number of queues equal to number of rps_cpus files",
			irqProcFile:   &procFileMock{},
			irqDeviceInfo: &deviceInfoMock{},
			ethtool: &ethtoolMock{
				driverName: func(string) (string, error) {
					return "ilwifi", nil
				},
			},
			before: func(fs afero.Fs) {
				for i := 0; i < 8; i++ {
					afero.WriteFile(fs, fmt.Sprintf("/sys/class/net/test0/queues/rx-%d/rps_cpus", i), []byte{}, 0o644)
				}
			},
			want: 8,
		},
		{
			name:          "Shall limit number of queues when they are limited by the driver",
			irqProcFile:   &procFileMock{},
			irqDeviceInfo: &deviceInfoMock{},
			ethtool: &ethtoolMock{
				driverName: func(string) (string, error) {
					return "ixgbevf", nil
				},
			},
			before: func(fs afero.Fs) {
				for i := 0; i < 8; i++ {
					afero.WriteFile(fs, fmt.Sprintf("/sys/class/net/test0/queues/rx-%d/rps_cpus", i), []byte{}, 0o644)
				}
			},
			want: 4,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := afero.NewMemMapFs()
			n := &nic{
				fs:            fs,
				irqProcFile:   tt.irqProcFile,
				irqDeviceInfo: tt.irqDeviceInfo,
				ethtool:       tt.ethtool,
				name:          "test0",
			}
			tt.before(fs)
			got, err := n.GetRxQueueCount()
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func Test_nic_GetNTupleStatus(t *testing.T) {
	tests := []struct {
		name          string
		irqProcFile   irq.ProcFile
		irqDeviceInfo irq.DeviceInfo
		ethtool       ethtool.EthtoolWrapper
		want          NTupleStatus
	}{
		{
			name: "Shall return not suported when iface does not support NTuples",
			ethtool: &ethtoolMock{
				features: func(string) (map[string]bool, error) {
					return map[string]bool{
						"other": true,
					}, nil
				},
			},
			want: NTupleNotSupported,
		},
		{
			name: "Shall return disabled when feature is present but disabled",
			ethtool: &ethtoolMock{
				features: func(string) (map[string]bool, error) {
					return map[string]bool{
						"ntuple": false,
					}, nil
				},
			},
			want: NTupleDisabled,
		},
		{
			name: "Shall return enabled when feature is present and enabled",
			ethtool: &ethtoolMock{
				features: func(string) (map[string]bool, error) {
					return map[string]bool{
						"ntuple": true,
					}, nil
				},
			},
			want: NTupleEnabled,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := &nic{
				fs:            afero.NewMemMapFs(),
				irqProcFile:   &procFileMock{},
				irqDeviceInfo: &deviceInfoMock{},
				ethtool:       tt.ethtool,
				name:          "test0",
			}
			got, err := n.GetNTupleStatus()
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}
