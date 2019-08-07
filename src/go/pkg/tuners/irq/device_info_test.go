package irq

import (
	"os"
	"reflect"
	"testing"
	"vectorized/pkg/utils"

	"github.com/spf13/afero"
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
	type fields struct {
		procFile ProcFile
		fs       afero.Fs
	}
	type args struct {
		irqConfigDir  string
		xenDeviceName string
	}
	tests := []struct {
		name    string
		fields  fields
		before  func(fields)
		args    args
		want    []int
		wantErr bool
	}{
		{
			name: "Shall return the IRQs when device is using MSI IRQs",
			fields: fields{
				fs: afero.NewMemMapFs(),
			},
			before: func(f fields) {
				_ = afero.WriteFile(f.fs, "/irq_config/dev1/msi_irqs/1", []byte{}, os.ModePerm)
				_ = afero.WriteFile(f.fs, "/irq_config/dev1/msi_irqs/2", []byte{}, os.ModePerm)
				_ = afero.WriteFile(f.fs, "/irq_config/dev1/msi_irqs/5", []byte{}, os.ModePerm)
				_ = afero.WriteFile(f.fs, "/irq_config/dev1/msi_irqs/8", []byte{}, os.ModePerm)
			},
			args: args{
				irqConfigDir: "/irq_config/dev1",
			},
			want:    []int{1, 2, 5, 8},
			wantErr: false,
		},
		{
			name: "Shall return the IRQs when device is using INT#x IRQs",
			fields: fields{
				fs: afero.NewMemMapFs(),
			},
			before: func(f fields) {
				_ = utils.WriteFileLines(f.fs, []string{"1", "2", "5", "8"},
					"/irq_config/dev1/irq")
			},
			args: args{
				irqConfigDir: "/irq_config/dev1",
			},
			want:    []int{1, 2, 5, 8},
			wantErr: false,
		},
		{
			name: "Shall return the IRQs from virtio device",
			fields: fields{
				fs: afero.NewMemMapFs(),
				procFile: &mockProcFile{
					getIRQProcFileLinesMap: func() (map[int]string, error) {
						return map[int]string{
							1: "1:     184233          0          0       7985   IO-APIC   1-edge      i8042",
							5: "5:          0          0          0          0   IO-APIC   5-edge      drv-virtio-1",
							8: "8:          1          0          0          0   IO-APIC   8-edge      rtc0"}, nil
					},
				},
			},
			before: func(f fields) {
				_ = utils.WriteFileLines(f.fs,
					[]string{"virtio:v00008086d000024DBsv0000103Csd0000006Abc01sc01i8A"},
					"/irq_config/dev1/modalias")
				_ = utils.WriteFileLines(f.fs,
					[]string{},
					"/irq_config/dev1/driver/drv-virtio-1")
			},
			args: args{
				irqConfigDir:  "/irq_config/dev1",
				xenDeviceName: "dev1",
			},
			want:    []int{5},
			wantErr: false,
		},
		{
			name: "Shall return the IRQs using XEN device name",
			fields: fields{
				fs: afero.NewMemMapFs(),
				procFile: &mockProcFile{
					getIRQProcFileLinesMap: func() (map[int]string, error) {
						return map[int]string{
							1: "1:     184233          0          0       7985   IO-APIC   1-edge      xen-dev1",
							5: "5:          0          0          0          0   IO-APIC   5-edge      drv-virtio-1",
							8: "8:          1          0          0          0   IO-APIC   8-edge      rtc0"}, nil
					},
				},
			},
			before: func(f fields) {
				_ = utils.WriteFileLines(f.fs,
					[]string{"xen:v00008086d000024DBsv0000103Csd0000006Abc01sc01i8A"},
					"/irq_config/dev1/modalias")
				_ = utils.WriteFileLines(f.fs,
					[]string{},
					"/irq_config/dev1/driver/drv-virtio-1")
			},
			args: args{
				irqConfigDir:  "/irq_config/dev1",
				xenDeviceName: "xen-dev1",
			},
			want:    []int{1},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.before(tt.fields)
			deviceInfo := NewDeviceInfo(
				tt.fields.fs,
				tt.fields.procFile,
			)
			got, err := deviceInfo.GetIRQs(tt.args.irqConfigDir,
				tt.args.xenDeviceName)
			if (err != nil) != tt.wantErr {
				t.Errorf("deviceInfo.GetIRQs() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("deviceInfo.GetIRQs() = %v, want %v", got, tt.want)
			}
		})
	}
}
