package filesystem

import (
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/spf13/afero"
)

func TestDirectoryIsWriteable(t *testing.T) {
	type args struct {
		fs   afero.Fs
		path string
	}
	tests := []struct {
		name    string
		args    args
		before  func(fs afero.Fs)
		want    bool
		wantErr bool
	}{
		{
			name: "Shall not return an error when directory is writable",
			args: args{
				fs:   afero.NewMemMapFs(),
				path: "/redpanda/data",
			},
			before: func(fs afero.Fs) {

			},
			wantErr: false,
			want:    true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.before(tt.args.fs)
			got, err := DirectoryIsWriteable(tt.args.fs, tt.args.path)
			if (err != nil) != tt.wantErr {
				t.Errorf("DirectoryIsWriteable() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("DirectoryIsWriteable() = %v, want %v", got, tt.want)
			}
		})
	}
}

var fsTab = `
#
# /etc/fstab
# Created by anaconda on Fri Mar 15 19:26:28 2019
#
# Accessible filesystems, by reference, are maintained under '/dev/disk/'.
# See man pages fstab(5), findfs(8), mount(8) and/or blkid(8) for more info.
#
# After editing this file, run 'systemctl daemon-reload' to update systemd
# units generated from this file.
#
/dev/mapper/fedora-root /                       ext4     defaults,x-systemd.device-timeout=0 0 0
UUID=fcb5a7ad-2b5e-4207-84c8-7d50e4d8215a /boot                   xfs     defaults        0 0
UUID=E51D-09E3          /boot/efi               vfat    umask=0077,shortname=winnt 0 2
/dev/mapper/fedora-home /home                   xfs     defaults,x-systemd.device-timeout=0 0 0

`

func Test_parseFsTab(t *testing.T) {
	type args struct {
		reader io.Reader
	}
	tests := []struct {
		name    string
		args    args
		want    []FsInfo
		wantErr bool
	}{
		{
			name: "Shall parse valid fs tab file",
			args: args{
				reader: strings.NewReader(fsTab),
			},
			want: []FsInfo{
				FsInfo{
					DeviceID:       "/dev/mapper/fedora-root",
					MountPoint:     "/",
					FilesystemType: "ext4",
				},
				FsInfo{
					DeviceID:       "UUID=fcb5a7ad-2b5e-4207-84c8-7d50e4d8215a",
					MountPoint:     "/boot",
					FilesystemType: "xfs",
				},
				FsInfo{
					DeviceID:       "UUID=E51D-09E3",
					MountPoint:     "/boot/efi",
					FilesystemType: "vfat",
				},
				FsInfo{
					DeviceID:       "/dev/mapper/fedora-home",
					MountPoint:     "/home",
					FilesystemType: "xfs",
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseFsTab(tt.args.reader)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseFsTab() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseFsTab() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetFilesystemType(t *testing.T) {
	type args struct {
		fs   afero.Fs
		path string
	}
	tests := []struct {
		name    string
		args    args
		before  func(afero.Fs)
		want    string
		wantErr bool
	}{
		{
			name: "Shall return correct filesystem name",
			args: args{
				fs:   afero.NewMemMapFs(),
				path: "/home/user/redpanda/data",
			},
			before: func(fs afero.Fs) {
				fs.MkdirAll("/etc", 0755)
				afero.WriteReader(fs, "/etc/fstab", strings.NewReader(fsTab))
			},
			want:    "xfs",
			wantErr: false,
		},
		{
			name: "Shall return correct filesystem name for root mount poing",
			args: args{
				fs:   afero.NewMemMapFs(),
				path: "/var/lib/redpanda/data",
			},
			before: func(fs afero.Fs) {
				fs.MkdirAll("/etc", 0755)
				afero.WriteReader(fs, "/etc/fstab", strings.NewReader(fsTab))
			},
			want:    "ext4",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.before(tt.args.fs)
			got, err := GetFilesystemType(tt.args.fs, tt.args.path)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetFilesystemType() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("GetFilesystemType() = %v, want %v", got, tt.want)
			}
		})
	}
}
