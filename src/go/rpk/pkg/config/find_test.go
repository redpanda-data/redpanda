// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

func TestFindConfig(t *testing.T) {
	tests := []struct {
		name    string
		before  func(fs afero.Fs)
		want    string
		wantErr bool
	}{
		{
			name:    "should return an error when config is not found",
			before:  func(afero.Fs) {},
			want:    "",
			wantErr: true,
		},
		{
			name: "should return config file from home directory",
			before: func(fs afero.Fs) {
				createConfigIn(fs, homeDir())
			},
			want: filepath.Join(homeDir(), "redpanda.yaml"),
		},
		{
			name: "should return config file from 'etc' directory",
			before: func(fs afero.Fs) {
				createConfigIn(fs, filepath.Join("/", "etc", "redpanda"))
			},
			want: filepath.Join("/", "etc", "redpanda", "redpanda.yaml"),
		},
		{
			name: "should return config file from current directory",
			before: func(fs afero.Fs) {
				createConfigIn(fs, currentDir())
			},
			want: filepath.Join(currentDir(), "redpanda.yaml"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := afero.NewMemMapFs()
			tt.before(fs)
			got, err := FindConfigFile(fs)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func createConfigIn(fs afero.Fs, path string) {
	fs.Create(filepath.Join(path, "redpanda.yaml"))
}

func currentDir() string {
	d, _ := os.Getwd()
	return d
}

func homeDir() string {
	d, _ := os.UserHomeDir()
	return d
}
