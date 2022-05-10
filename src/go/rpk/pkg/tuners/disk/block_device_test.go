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
	"path/filepath"
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/utils"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

func Test_deviceFromSystemPath(t *testing.T) {
	tests := []struct {
		name    string
		syspath string
		before  func(afero.Fs, string)
		want    BlockDevice
	}{
		{
			name:    "shall return simple device",
			syspath: "/sys/devices/pci0000:00/0000:00:1d.0/nvme/nvme0",
			before: func(fs afero.Fs, syspath string) {
				ueventFileLines := []string{"DEVNAME=node-name"}
				fs.MkdirAll(syspath, 0o755)
				utils.WriteFileLines(fs, ueventFileLines,
					filepath.Join(syspath, "uevent"))
			},
			want: &blockDevice{
				syspath: "/sys/devices/pci0000:00/0000:00:1d.0/nvme/nvme0",
				devnode: "/dev/node-name",
				parent:  nil,
			},
		},
		{
			name:    "shall return device with its parent",
			syspath: "/sys/devices/pci0000:00/0000:00:1d.0/nvme/nvme0/nvme0n1",
			before: func(fs afero.Fs, syspath string) {
				ueventFileLines := []string{"DEVNAME=child"}
				fs.MkdirAll(syspath, 0o755)
				utils.WriteFileLines(fs, ueventFileLines,
					filepath.Join(syspath, "uevent"))
				parentPath := "/sys/devices/pci0000:00/0000:00:1d.0/nvme/nvme0"
				parentUeventFileLines := []string{"DEVNAME=parent"}
				utils.WriteFileLines(fs, parentUeventFileLines,
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
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := afero.NewMemMapFs()
			tt.before(fs, tt.syspath)
			got, err := deviceFromSystemPath(tt.syspath, fs)
			require.NoError(t, err)
			require.Exactly(t, tt.want, got)
		})
	}
}
