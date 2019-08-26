package network

import (
	"fmt"
	"reflect"
	"testing"
	"vectorized/pkg/tuners/ethtool"
	"vectorized/pkg/tuners/irq"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
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
	//given
	fs := afero.NewMemMapFs()
	procFile := &procFileMock{}
	deviceInfo := &deviceInfoMock{}
	nic := NewNic(fs, procFile, deviceInfo, &ethtoolMock{}, "test0")
	afero.WriteFile(fs, "/sys/class/net/bond_masters", []byte(fmt.Sprintln("test0")), 0644)
	//when
	bond := nic.IsBondIface()
	//then
	assert.True(t, bond)
}

func Test_nic_Slaves_ReturnAllSlavesOfAnInterface(t *testing.T) {
	//given
	fs := afero.NewMemMapFs()
	procFile := &procFileMock{}
	deviceInfo := &deviceInfoMock{}
	nic := NewNic(fs, procFile, deviceInfo, &ethtoolMock{}, "test0")
	afero.WriteFile(fs, "/sys/class/net/bond_masters", []byte(fmt.Sprintln("test0")), 0644)
	afero.WriteFile(fs, "/sys/class/net/test0/bond/slaves", []byte(fmt.Sprint("sl0\nsl1\nsl2")), 0644)
	//when
	slaves, err := nic.Slaves()
	//then
	assert.NoError(t, err)
	assert.Len(t, slaves, 3)
	assert.Equal(t, slaves[0].Name(), "sl0")
	assert.Equal(t, slaves[1].Name(), "sl1")
	assert.Equal(t, slaves[2].Name(), "sl2")
}

func Test_nic_Slaves_ReturnEmptyForNotBondInterface(t *testing.T) {
	//given
	fs := afero.NewMemMapFs()
	procFile := &procFileMock{}
	deviceInfo := &deviceInfoMock{}
	nic := NewNic(fs, procFile, deviceInfo, &ethtoolMock{}, "test0")
	//when
	slaves, err := nic.Slaves()
	//then
	assert.NoError(t, err)
	assert.Empty(t, slaves)
}

func Test_nic_GetIRQs(t *testing.T) {
	type fields struct {
		fs            afero.Fs
		irqProcFile   irq.ProcFile
		irqDeviceInfo irq.DeviceInfo
		ethtool       ethtool.EthtoolWrapper
		name          string
	}
	tests := []struct {
		name    string
		fields  fields
		want    []int
		wantErr bool
	}{
		{
			name: "Shall return all device IRQs when there are not fast paths",
			fields: fields{
				fs: afero.NewMemMapFs(),
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
				ethtool: &ethtoolMock{},
				name:    "test0",
			},
			want:    []int{54, 56, 58},
			wantErr: false,
		},
		{
			name: "Shall return fast path IRQs only sorted by queue number",
			fields: fields{
				fs: afero.NewMemMapFs(),
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
				ethtool: &ethtoolMock{},
				name:    "test0",
			},
			want:    []int{95, 94, 93, 92},
			wantErr: false,
		},
		{
			name: "Fdir fast path IRQs should be moved to the end of list",
			fields: fields{
				fs: afero.NewMemMapFs(),
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
				ethtool: &ethtoolMock{},
				name:    "test0",
			},
			want:    []int{95, 94, 93, 92, 96},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := &nic{
				fs:            tt.fields.fs,
				irqProcFile:   tt.fields.irqProcFile,
				irqDeviceInfo: tt.fields.irqDeviceInfo,
				ethtool:       tt.fields.ethtool,
				name:          tt.fields.name,
			}
			got, err := n.GetIRQs()
			if (err != nil) != tt.wantErr {
				t.Errorf("nic.GetIRQs() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("nic.GetIRQs() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_nic_GetMaxRxQueueCount(t *testing.T) {
	type fields struct {
		Nic           Nic
		fs            afero.Fs
		irqProcFile   irq.ProcFile
		irqDeviceInfo irq.DeviceInfo
		ethtool       ethtool.EthtoolWrapper
		name          string
	}
	tests := []struct {
		name    string
		fields  fields
		want    int
		wantErr bool
	}{
		{
			name: "Shall return correct max queues for ixgbe driver",
			fields: fields{
				fs:            afero.NewMemMapFs(),
				irqProcFile:   &procFileMock{},
				irqDeviceInfo: &deviceInfoMock{},
				ethtool: &ethtoolMock{
					driverName: func(string) (string, error) {
						return "ixgbe", nil
					},
				},
				name: "test0",
			},
			want:    16,
			wantErr: false,
		},
		{
			name: "Shall return max int when driver is unknown",
			fields: fields{
				fs:            afero.NewMemMapFs(),
				irqProcFile:   &procFileMock{},
				irqDeviceInfo: &deviceInfoMock{},
				ethtool: &ethtoolMock{
					driverName: func(string) (string, error) {
						return "iwlwifi", nil
					},
				},
				name: "test0",
			},
			want:    maxInt,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := &nic{
				Nic:           tt.fields.Nic,
				fs:            tt.fields.fs,
				irqProcFile:   tt.fields.irqProcFile,
				irqDeviceInfo: tt.fields.irqDeviceInfo,
				ethtool:       tt.fields.ethtool,
				name:          tt.fields.name,
			}
			got, err := n.GetMaxRxQueueCount()
			if (err != nil) != tt.wantErr {
				t.Errorf("nic.GetMaxRxQueueCount() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("nic.GetMaxRxQueueCount() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_nic_GetRxQueueCount(t *testing.T) {
	type fields struct {
		Nic           Nic
		fs            afero.Fs
		irqProcFile   irq.ProcFile
		irqDeviceInfo irq.DeviceInfo
		ethtool       ethtool.EthtoolWrapper
		name          string
		before        func(fs afero.Fs)
	}
	tests := []struct {
		name    string
		fields  fields
		want    int
		wantErr bool
	}{
		{
			name: "Shall return len(IRQ) when RPS is disabled and driver is not limiting queus number",
			fields: fields{
				fs: afero.NewMemMapFs(),
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
				name: "test0",
				before: func(fs afero.Fs) {
				},
			},
			want:    5,
			wantErr: false,
		},
		{
			name: "Shall return number of queues equal to number of rps_cpus files",
			fields: fields{
				fs:            afero.NewMemMapFs(),
				irqProcFile:   &procFileMock{},
				irqDeviceInfo: &deviceInfoMock{},
				ethtool: &ethtoolMock{
					driverName: func(string) (string, error) {
						return "ilwifi", nil
					},
				},
				name: "test0",
				before: func(fs afero.Fs) {
					for i := 0; i < 8; i++ {
						afero.WriteFile(fs, fmt.Sprintf("/sys/class/net/test0/queues/rx-%d/rps_cpus", i), []byte{}, 0644)
					}
				},
			},
			want:    8,
			wantErr: false,
		},
		{
			name: "Shall limit number of queues when they are limited by the driver",
			fields: fields{
				fs:            afero.NewMemMapFs(),
				irqProcFile:   &procFileMock{},
				irqDeviceInfo: &deviceInfoMock{},
				ethtool: &ethtoolMock{
					driverName: func(string) (string, error) {
						return "ixgbevf", nil
					},
				},
				name: "test0",
				before: func(fs afero.Fs) {
					for i := 0; i < 8; i++ {
						afero.WriteFile(fs, fmt.Sprintf("/sys/class/net/test0/queues/rx-%d/rps_cpus", i), []byte{}, 0644)
					}
				},
			},
			want:    4,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := &nic{
				Nic:           tt.fields.Nic,
				fs:            tt.fields.fs,
				irqProcFile:   tt.fields.irqProcFile,
				irqDeviceInfo: tt.fields.irqDeviceInfo,
				ethtool:       tt.fields.ethtool,
				name:          tt.fields.name,
			}
			tt.fields.before(tt.fields.fs)
			got, err := n.GetRxQueueCount()
			if (err != nil) != tt.wantErr {
				t.Errorf("nic.GetRxQueueCount() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("nic.GetRxQueueCount() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_nic_GetNTupleStatus(t *testing.T) {
	type fields struct {
		Nic           Nic
		fs            afero.Fs
		irqProcFile   irq.ProcFile
		irqDeviceInfo irq.DeviceInfo
		ethtool       ethtool.EthtoolWrapper
		name          string
	}
	tests := []struct {
		name    string
		fields  fields
		want    NTupleStatus
		wantErr bool
	}{
		{
			name: "Shall return not suported when iface does not support NTuples",
			fields: fields{
				fs:            afero.NewMemMapFs(),
				irqProcFile:   &procFileMock{},
				irqDeviceInfo: &deviceInfoMock{},
				ethtool: &ethtoolMock{
					features: func(string) (map[string]bool, error) {
						return map[string]bool{
							"other": true,
						}, nil
					},
				},
				name: "test0",
			},
			want:    NTupleNotSupported,
			wantErr: false,
		},
		{
			name: "Shall return disabled when feature is present but disabled",
			fields: fields{
				fs:            afero.NewMemMapFs(),
				irqProcFile:   &procFileMock{},
				irqDeviceInfo: &deviceInfoMock{},
				ethtool: &ethtoolMock{
					features: func(string) (map[string]bool, error) {
						return map[string]bool{
							"ntuple": false,
						}, nil
					},
				},
				name: "test0",
			},
			want:    NTupleDisabled,
			wantErr: false,
		},
		{
			name: "Shall return enabled when feature is present and enabled",
			fields: fields{
				fs:            afero.NewMemMapFs(),
				irqProcFile:   &procFileMock{},
				irqDeviceInfo: &deviceInfoMock{},
				ethtool: &ethtoolMock{
					features: func(string) (map[string]bool, error) {
						return map[string]bool{
							"ntuple": true,
						}, nil
					},
				},
				name: "test0",
			},
			want:    NTupleEnabled,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := &nic{
				Nic:           tt.fields.Nic,
				fs:            tt.fields.fs,
				irqProcFile:   tt.fields.irqProcFile,
				irqDeviceInfo: tt.fields.irqDeviceInfo,
				ethtool:       tt.fields.ethtool,
				name:          tt.fields.name,
			}
			got, err := n.GetNTupleStatus()
			if (err != nil) != tt.wantErr {
				t.Errorf("nic.GetNTupleStatus() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("nic.GetNTupleStatus() = %v, want %v", got, tt.want)
			}
		})
	}
}
