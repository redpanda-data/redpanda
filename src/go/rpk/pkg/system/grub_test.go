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
	"crypto/md5"
	"encoding/hex"
	"fmt"
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
		before  func(afero.Fs, []string)
		grubCfg []string
		opt     []string
		wantErr bool
		check   func(afero.Fs, []string)
	}{
		{
			name:   "Shall add new value only flag to GRUB cfg",
			before: persistGrubConfig,
			grubCfg: []string{
				"GRUB_TIMEOUT=5",
				"GRUB_DISTRIBUTOR=\"$(sed 's, release .*$,,g' /etc/system-release)\"",
				"GRUB_TERMINAL_OUTPUT=\"console\"",
				"GRUB_DEFAULT=saved",
				"GRUB_CMDLINE_LINUX=\"resume=/dev/mapper/fedora-swap rd.lvm.lv=fedora/root\"",
				"GRUB_DISABLE_SUBMENU=true",
			},
			opt:     []string{"noht"},
			wantErr: false,
			check: func(fs afero.Fs, grubCfg []string) {
				md5 := calcMd5(grubCfg)
				backupName := fmt.Sprintf("/etc/default/grub.vectorized.%s.bk", md5)
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
			name:   "Shall add new key/value pair flag to GRUB cfg",
			before: persistGrubConfig,
			grubCfg: []string{
				"GRUB_TIMEOUT=5",
				"GRUB_DISTRIBUTOR=\"$(sed 's, release .*$,,g' /etc/system-release)\"",
				"GRUB_TERMINAL_OUTPUT=\"console\"",
				"GRUB_DEFAULT=saved",
				"GRUB_CMDLINE_LINUX=\"resume=/dev/mapper/fedora-swap rd.lvm.lv=fedora/root\"",
				"GRUB_DISABLE_SUBMENU=true",
			},
			opt:     []string{"some_opt=2"},
			wantErr: false,
			check: func(fs afero.Fs, grubCfg []string) {
				md5 := calcMd5(grubCfg)
				backupName := fmt.Sprintf("/etc/default/grub.vectorized.%s.bk", md5)
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
			name:   "Shall not add the same value only flag twice",
			before: persistGrubConfig,
			grubCfg: []string{
				"GRUB_TIMEOUT=5",
				"GRUB_DISTRIBUTOR=\"$(sed 's, release .*$,,g' /etc/system-release)\"",
				"GRUB_TERMINAL_OUTPUT=\"console\"",
				"GRUB_DEFAULT=saved",
				"GRUB_CMDLINE_LINUX=\"resume=/dev/mapper/fedora-swap noht rd.lvm.lv=fedora/root\"",
				"GRUB_DISABLE_SUBMENU=true",
			},
			opt:     []string{"noht"},
			wantErr: false,
			check: func(fs afero.Fs, grubCfg []string) {
				md5 := calcMd5(grubCfg)
				backupName := fmt.Sprintf("/etc/default/grub.vectorized.%s.bk", md5)
				backupPresent, _ := afero.Exists(fs, backupName)
				require.Equal(t, backupPresent, false)
				lines, _ := utils.ReadFileLines(fs, "/etc/default/grub")
				require.Len(t, lines, 6)
				opts := getGrubCmdLineOptsLine(lines)
				require.Len(t, opts, 3)
			},
		},
		{
			name:   "Shall update the option value if it is already present",
			before: persistGrubConfig,
			grubCfg: []string{
				"GRUB_TIMEOUT=5",
				"GRUB_DISTRIBUTOR=\"$(sed 's, release .*$,,g' /etc/system-release)\"",
				"GRUB_TERMINAL_OUTPUT=\"console\"",
				"GRUB_DEFAULT=saved",
				"GRUB_CMDLINE_LINUX=\"resume=/dev/mapper/fedora-swap some_opt=1 rd.lvm.lv=fedora/root\"",
				"GRUB_DISABLE_SUBMENU=true",
			},
			opt:     []string{"some_opt=2"},
			wantErr: false,
			check: func(fs afero.Fs, grubCfg []string) {
				md5 := calcMd5(grubCfg)
				backupName := fmt.Sprintf("/etc/default/grub.vectorized.%s.bk", md5)
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
			tt.before(fs, tt.grubCfg)
			err := grub.AddCommandLineOptions(tt.opt)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
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

func calcMd5(lines []string) string {
	var data []byte
	for _, line := range lines {
		data = append(data, []byte(line+"\n")...)
	}
	hash := md5.Sum(data)
	return hex.EncodeToString(hash[:16])
}

func persistGrubConfig(fs afero.Fs, grubCfg []string) {
	utils.WriteFileLines(fs, grubCfg, "/etc/default/grub")
}
