// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package system

import (
	"testing"
	"time"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/executors"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/utils"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

func TestGrubAddCommandLineOptions(t *testing.T) {
	tests := []struct {
		name    string
		grubCfg []string
		opt     []string
		check   func(afero.Fs, []string)
	}{
		{
			name: "Shall add new value only flag to GRUB cfg",
			grubCfg: []string{
				"GRUB_TIMEOUT=5",
				"GRUB_DISTRIBUTOR=\"$(sed 's, release .*$,,g' /etc/system-release)\"",
				"GRUB_TERMINAL_OUTPUT=\"console\"",
				"GRUB_DEFAULT=saved",
				"GRUB_CMDLINE_LINUX=\"resume=/dev/mapper/fedora-swap rd.lvm.lv=fedora/root\"",
				"GRUB_DISABLE_SUBMENU=true",
			},
			opt: []string{"noht"},
			check: func(fs afero.Fs, grubCfg []string) {
				backupName := "/etc/default/grub.vectorized.2c349a84043328ae3a9f2d021ff143c3.bk"
				backup, err := utils.ReadFileLines(fs, backupName)
				require.Nil(t, err)
				require.Equal(t, grubCfg, backup)
				lines, _ := utils.ReadFileLines(fs, "/etc/default/grub")
				require.Len(t, lines, 6)
				opts := getGrubCmdLineOptsLine(lines)
				require.Len(t, opts, 3)
				require.Contains(t, opts, "noht")
			},
		},
		{
			name: "Shall add new key/value pair flag to GRUB cfg",
			grubCfg: []string{
				"GRUB_TIMEOUT=5",
				"GRUB_DISTRIBUTOR=\"$(sed 's, release .*$,,g' /etc/system-release)\"",
				"GRUB_TERMINAL_OUTPUT=\"console\"",
				"GRUB_DEFAULT=saved",
				"GRUB_CMDLINE_LINUX=\"resume=/dev/mapper/fedora-swap rd.lvm.lv=fedora/root\"",
				"GRUB_DISABLE_SUBMENU=true",
			},
			opt: []string{"some_opt=2"},
			check: func(fs afero.Fs, grubCfg []string) {
				backupName := "/etc/default/grub.vectorized.2c349a84043328ae3a9f2d021ff143c3.bk"
				backup, err := utils.ReadFileLines(fs, backupName)
				require.Nil(t, err)
				require.Equal(t, grubCfg, backup)
				lines, _ := utils.ReadFileLines(fs, "/etc/default/grub")
				opts := getGrubCmdLineOptsLine(lines)
				require.Len(t, opts, 3)
				require.Contains(t, opts, "some_opt=2")
			},
		},
		{
			name: "Shall not add the same value only flag twice",
			grubCfg: []string{
				"GRUB_TIMEOUT=5",
				"GRUB_DISTRIBUTOR=\"$(sed 's, release .*$,,g' /etc/system-release)\"",
				"GRUB_TERMINAL_OUTPUT=\"console\"",
				"GRUB_DEFAULT=saved",
				"GRUB_CMDLINE_LINUX=\"resume=/dev/mapper/fedora-swap noht rd.lvm.lv=fedora/root\"",
				"GRUB_DISABLE_SUBMENU=true",
			},
			opt: []string{"noht"},
			check: func(fs afero.Fs, grubCfg []string) {
				backupName := "/etc/default/grub.vectorized.58af885fa59687a5d6184d34945e05c1.bk"
				backupPresent, _ := afero.Exists(fs, backupName)
				require.Equal(t, backupPresent, false)
				lines, _ := utils.ReadFileLines(fs, "/etc/default/grub")
				require.Len(t, lines, 6)
				opts := getGrubCmdLineOptsLine(lines)
				require.Len(t, opts, 3)
			},
		},
		{
			name: "Shall update the option value if it is already present",
			grubCfg: []string{
				"GRUB_TIMEOUT=5",
				"GRUB_DISTRIBUTOR=\"$(sed 's, release .*$,,g' /etc/system-release)\"",
				"GRUB_TERMINAL_OUTPUT=\"console\"",
				"GRUB_DEFAULT=saved",
				"GRUB_CMDLINE_LINUX=\"resume=/dev/mapper/fedora-swap some_opt=1 rd.lvm.lv=fedora/root\"",
				"GRUB_DISABLE_SUBMENU=true",
			},
			opt: []string{"some_opt=2"},
			check: func(fs afero.Fs, grubCfg []string) {
				backupName := "/etc/default/grub.vectorized.fc4df103de9bce221b735953fc36d4ad.bk"
				lines, _ := utils.ReadFileLines(fs, "/etc/default/grub")
				backup, err := utils.ReadFileLines(fs, backupName)
				require.Nil(t, err)
				require.Equal(t, grubCfg, backup)
				require.Len(t, lines, 6)
				opts := getGrubCmdLineOptsLine(lines)
				require.Len(t, opts, 3)
				require.Contains(t, opts, "some_opt=2")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := afero.NewMemMapFs()
			grub := NewGrub(nil, nil, fs, executors.NewDirectExecutor(), time.Duration(10)*time.Second)
			utils.WriteFileLines(fs, tt.grubCfg, "/etc/default/grub")
			err := grub.AddCommandLineOptions(tt.opt)
			require.NoError(t, err)
			tt.check(fs, tt.grubCfg)
		})
	}
}

func TestOptionsNeedChange(t *testing.T) {
	tests := []struct {
		name      string
		current   []string
		requested []string
		want      bool
	}{
		{
			name:      "shall return false for the same sets of options",
			current:   []string{"opt1=val1", "noht", "opt_2=val_2"},
			requested: []string{"opt1=val1", "noht", "opt_2=val_2"},
			want:      false,
		},
		{
			name:      "shall return true as flag option differs",
			current:   []string{"opt1=val1", "opt_2=val_2"},
			requested: []string{"opt1=val1", "noht", "opt_2=val_2"},
			want:      true,
		},
		{
			name:      "shall return true as key/value option differs",
			current:   []string{"opt1=val1", "noht", "opt_2=val_2"},
			requested: []string{"opt1=val1", "noht", "opt_2=val_3"},
			want:      true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := optionsNeedChange(tt.current, tt.requested)
			require.Equal(t, tt.want, got)
		})
	}
}

func getGrubCmdLineOptsLine(configLines []string) []string {
	for _, line := range configLines {
		if opts := matchAndSplitCmdOptions(line); opts != nil {
			return opts
		}
	}
	return nil
}
