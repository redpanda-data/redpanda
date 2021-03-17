// Copyright 2020 Vectorized, Inc.
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
			name: "should return config file from parent directory",
			before: func(fs afero.Fs) {
				currentDir := currentDir()
				fs.MkdirAll(currentDir, 0755)
				createConfigIn(fs, filepath.Dir(currentDir))
			},
			want: filepath.Join(filepath.Dir(currentDir()), "redpanda.yaml"),
		},
		{
			name: "should return config file from 'etc' directory",
			before: func(fs afero.Fs) {
				createConfigIn(fs, "/etc/redpanda")
				currentDir := currentDir()
				fs.MkdirAll(currentDir, 0755)
				createConfigIn(fs, filepath.Dir(currentDir))
			},
			want: "/etc/redpanda/redpanda.yaml",
		},
		{
			name: "should return config file from current directory",
			before: func(fs afero.Fs) {
				createConfigIn(fs, "/etc/redpanda")
				currentDir := currentDir()
				fs.MkdirAll(currentDir, 0755)
				createConfigIn(fs, filepath.Dir(currentDir))
				createConfigIn(fs, currentDir)
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
