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
		name             string
		grubInitFilename string
		grubWantFilename string
		opt              []string
		check            func(afero.Fs, []string)
	}{
		{
			name:             "Shall add new value only flag to GRUB cfg",
			grubInitFilename: "testdata/grub-00-init",
			grubWantFilename: "testdata/grub-00-want",
			opt:              []string{"noht"},
			check: func(fs afero.Fs, grubCfg []string) {
				backupName := "/etc/default/grub.vectorized.9be9f2dfe19f13b03e09fcc75648d4ec.bk"
				backup, err := utils.ReadFileLines(fs, backupName)
				require.NoError(t, err)
				require.Equal(t, grubCfg, backup)
			},
		},
		{
			name:             "Shall add new key/value pair flag to GRUB cfg",
			grubInitFilename: "testdata/grub-01-init",
			grubWantFilename: "testdata/grub-01-want",
			opt:              []string{"some_opt=2"},
			check: func(fs afero.Fs, grubCfg []string) {
				backupName := "/etc/default/grub.vectorized.9be9f2dfe19f13b03e09fcc75648d4ec.bk"
				backup, err := utils.ReadFileLines(fs, backupName)
				require.NoError(t, err)
				require.Equal(t, grubCfg, backup)
			},
		},
		{
			name:             "Shall not add the same value only flag twice",
			grubInitFilename: "testdata/grub-02-init",
			grubWantFilename: "testdata/grub-02-want",
			opt:              []string{"noht"},
			check: func(fs afero.Fs, grubCfg []string) {
				backupName := "/etc/default/grub.vectorized.dc06cc27c72e64f17e5dd411d5f2b1b2.bk"
				backupPresent, _ := afero.Exists(fs, backupName)
				require.Equal(t, backupPresent, false)
			},
		},
		{
			name:             "Shall update the option value if it is already present",
			grubInitFilename: "testdata/grub-03-init",
			grubWantFilename: "testdata/grub-03-want",
			opt:              []string{"some_opt=2"},
			check: func(fs afero.Fs, grubCfg []string) {
				backupName := "/etc/default/grub.vectorized.f88adb8d4d821d657e1d24d1d87a854e.bk"
				backup, err := utils.ReadFileLines(fs, backupName)
				require.NoError(t, err)
				require.Equal(t, grubCfg, backup)
			},
		},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("test %02d %s", i, tt.name), func(t *testing.T) {
			fs := afero.NewMemMapFs()
			grub := NewGrub(nil, nil, fs, executors.NewDirectExecutor(), time.Duration(10)*time.Second)
			osfs := afero.NewOsFs()
			grubCfg, err := utils.ReadFileLines(osfs, tt.grubInitFilename)
			require.NoError(t, err)
			utils.WriteFileLines(fs, grubCfg, "/etc/default/grub")
			err = grub.AddCommandLineOptions(tt.opt)
			require.NoError(t, err)
			tt.check(fs, grubCfg)
			grubGot, err := utils.ReadFileLines(fs, "/etc/default/grub")
			require.NoError(t, err)
			grubWant, err := utils.ReadFileLines(osfs, tt.grubWantFilename)
			require.NoError(t, err)
			require.Equal(t, grubWant, grubGot)
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
