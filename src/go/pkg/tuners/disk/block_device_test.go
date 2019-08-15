package disk

import (
	"path/filepath"
	"reflect"
	"testing"
	"vectorized/pkg/utils"

	"github.com/spf13/afero"
)

func Test_deviceFromSystemPath(t *testing.T) {
	type args struct {
		syspath string
		fs      afero.Fs
	}
	tests := []struct {
		name    string
		args    args
		before  func(args)
		want    BlockDevice
		wantErr bool
	}{
		{
			name: "shall return simple device",
			args: args{
				syspath: "/sys/devices/pci0000:00/0000:00:1d.0/nvme/nvme0",
				fs:      afero.NewMemMapFs()},
			before: func(args args) {
				ueventFileLines := []string{"DEVNAME=node-name"}
				args.fs.MkdirAll(args.syspath, 0755)
				utils.WriteFileLines(args.fs, ueventFileLines,
					filepath.Join(args.syspath, "uevent"))
			},
			want: &blockDevice{
				syspath: "/sys/devices/pci0000:00/0000:00:1d.0/nvme/nvme0",
				devnode: "/dev/node-name",
				parent:  nil,
			},
			wantErr: false,
		},
		{
			name: "shall return device with its parent",
			args: args{
				syspath: "/sys/devices/pci0000:00/0000:00:1d.0/nvme/nvme0/nvme0n1",
				fs:      afero.NewMemMapFs()},
			before: func(args args) {
				ueventFileLines := []string{"DEVNAME=child"}
				args.fs.MkdirAll(args.syspath, 0755)
				utils.WriteFileLines(args.fs, ueventFileLines,
					filepath.Join(args.syspath, "uevent"))
				parentPath := "/sys/devices/pci0000:00/0000:00:1d.0/nvme/nvme0"
				parentUeventFileLines := []string{"DEVNAME=parent"}
				utils.WriteFileLines(args.fs, parentUeventFileLines,
					filepath.Join(parentPath, "uevent"))
			},
			want: &blockDevice{
				syspath: "/sys/devices/pci0000:00/0000:00:1d.0/nvme/nvme0/nvme0n1",
				devnode: "/dev/child",
				parent: &blockDevice{
					syspath: "/sys/devices/pci0000:00/0000:00:1d.0/nvme/nvme0",
					devnode: "/dev/parent",
					parent:  nil,
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.before(tt.args)
			got, err := deviceFromSystemPath(tt.args.syspath, tt.args.fs)
			if (err != nil) != tt.wantErr {
				t.Errorf("deviceFromSystemPath() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("deviceFromSystemPath() = %v, want %v", got, tt.want)
			}
		})
	}
}
