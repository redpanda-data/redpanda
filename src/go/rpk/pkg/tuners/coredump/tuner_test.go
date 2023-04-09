// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package coredump

import (
	"os"
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/executors"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

func TestTune(t *testing.T) {
	testDir := "/var/lib/redpanda/coredumps"
	tests := []struct {
		name string
		pre  func(afero.Fs) error
	}{
		{
			name: "it should install the coredump config file",
		},
		{
			name: "it should not fail to install if the coredump config file already exists",
			pre: func(fs afero.Fs) error {
				_, err := fs.Create(corePatternFilePath)
				return err
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := afero.NewMemMapFs()
			if tt.pre != nil {
				err := tt.pre(fs)
				require.NoError(t, err)
			}
			tuner := NewCoredumpTuner(fs, testDir, executors.NewDirectExecutor())
			res := tuner.Tune()
			require.NoError(t, res.Error())
			pattern, err := fs.Open(corePatternFilePath)
			require.NoError(t, err)
			script, err := fs.Open(scriptFilePath)
			require.NoError(t, err)
			info, err := script.Stat()
			require.NoError(t, err)
			// Check that the script is world-readable, writable and executable
			expectedMode := os.FileMode(int(0o777))
			require.Equal(t, expectedMode, info.Mode())
			expectedScript, err := renderTemplate(coredumpScriptTmpl, testDir)
			require.NoError(t, err)
			buf := make([]byte, len(expectedScript))
			_, err = script.Read(buf)
			require.NoError(t, err)
			actualScript := string(buf)
			require.Equal(t, expectedScript, actualScript)
			buf = make([]byte, len(coredumpPattern))
			_, err = pattern.Read(buf)
			require.NoError(t, err)
			actualPattern := string(buf)
			require.Equal(t, coredumpPattern, actualPattern)
		})
	}
}
