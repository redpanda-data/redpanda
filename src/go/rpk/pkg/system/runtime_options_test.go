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
	"path/filepath"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

func TestReadRuntineOptions(t *testing.T) {
	path := "/proc/test"
	tests := []struct {
		name    string
		opts    []byte
		want    *RuntimeOptions
		wantErr bool
	}{
		{
			name: "shall return correct set of options",
			opts: []byte("opt1 opt2 [opt3] opt4\n"),
			want: &RuntimeOptions{
				optionsMap: map[string]bool{
					"opt1": false,
					"opt2": false,
					"opt3": true,
					"opt4": false,
				},
			},
		},
		{
			name: "shall return correct set of options when last one is active",
			opts: []byte("opt1 opt2 [opt3]\n"),
			want: &RuntimeOptions{
				optionsMap: map[string]bool{
					"opt1": false,
					"opt2": false,
					"opt3": true,
				},
			},
		},
		{
			name: "shall return the option when there is only one string",
			opts: []byte("opt1\n"),
			want: &RuntimeOptions{
				optionsMap: map[string]bool{"opt1": true},
			},
		},
		{
			name:    "shall return error when there are more than one line in file",
			opts:    []byte("opt1 opt2 [opt3]\n second line\n"),
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := afero.NewMemMapFs()
			err := fs.MkdirAll(filepath.Dir(path), 0o755)
			require.NoError(t, err)
			err = afero.WriteFile(fs, path, tt.opts, 0o644)
			require.NoError(t, err)
			got, err := ReadRuntineOptions(fs, path)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Exactly(t, tt.want, got)
		})
	}
}
