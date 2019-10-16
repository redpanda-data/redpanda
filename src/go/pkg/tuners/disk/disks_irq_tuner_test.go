package disk

import (
	"reflect"
	"testing"
	"vectorized/pkg/tuners/irq"
)

type cpuMasksMock struct {
	irq.CpuMasks
	baseCpuMask              func(string) (string, error)
	cpuMaskForIRQs           func(irq.Mode, string) (string, error)
	getIRQsDistributionMasks func([]int, string) (map[int]string, error)
}

func (m *cpuMasksMock) BaseCpuMask(cpuMask string) (string, error) {
	return m.baseCpuMask(cpuMask)
}

func (m *cpuMasksMock) CpuMaskForIRQs(
	mode irq.Mode, cpuMask string,
) (string, error) {
	return m.cpuMaskForIRQs(mode, cpuMask)
}

func (m *cpuMasksMock) GetIRQsDistributionMasks(
	IRQs []int, cpuMask string,
) (map[int]string, error) {
	return m.getIRQsDistributionMasks(IRQs, cpuMask)
}

func TestGetExpectedIRQsDistribution(t *testing.T) {
	type args struct {
		devices      []string
		mode         irq.Mode
		cpuMask      string
		blockDevices BlockDevices
		cpuMasks     irq.CpuMasks
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
					getDiskInfoByType: func([]string) (map[diskType]devicesIRQs, error) {
						return map[diskType]devicesIRQs{
							nonNvme: devicesIRQs{
								devices: []string{"dev1"},
								irqs:    []int{10},
							},
							nvme: devicesIRQs{
								devices: []string{"dev1"},
								irqs:    []int{12, 15, 18, 24}},
						}, nil
					},
				},
				cpuMasks: &cpuMasksMock{
					baseCpuMask: func(string) (string, error) {
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
			if (err != nil) != tt.wantErr {
				t.Errorf("GetExpectedIRQsDistribution() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetExpectedIRQsDistribution() = %v, want %v", got, tt.want)
			}
		})
	}
}
