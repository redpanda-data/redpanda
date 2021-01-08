// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package wasm

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

func checkDirsExist(fs afero.Fs, t *testing.T, paths []string) {
	for _, path := range paths {
		exists, err := afero.DirExists(fs, filepath.Join(path))
		require.NoError(t, err)
		require.True(t, exists)
	}
}

func checkFilesExist(fs afero.Fs, t *testing.T, paths []string) {
	for _, path := range paths {
		exists, err := afero.Exists(fs, filepath.Join(path))
		require.NoError(t, err)
		require.True(t, exists)
	}
}

func checkGeneratedFiles(fs afero.Fs, t *testing.T, basePath string) {
	checkDirsExist(fs, t, []string{
		basePath,
		filepath.Join(basePath, "src"),
		filepath.Join(basePath, "test"),
	})

	checkFilesExist(fs, t, []string{
		filepath.Join(basePath, "package.json"),
		filepath.Join(basePath, "webpack.js"),
		filepath.Join(basePath, "src", "wasm.js"),
		filepath.Join(basePath, "test", "wasm.test.js"),
	})
}

func TestWasmCommand(t *testing.T) {
	path, err := os.Getwd()
	require.NoError(t, err)
	tests := []struct {
		name		string
		args		[]string
		before		func(afero.Fs) error
		check		func(fs afero.Fs, t *testing.T)
		expectedErrMsg	string
	}{
		{
			name:	"should create an npm template with its folder",
			args:	[]string{"generate"},
			check: func(fs afero.Fs, t *testing.T) {
				dir := filepath.Join(path, "wasm")
				checkGeneratedFiles(fs, t, dir)
			},
		},
		{
			name:	"should create the project at the given path",
			args:	[]string{"generate", "--dir", "./newFolder"},
			check: func(fs afero.Fs, t *testing.T) {
				absolutePath, err := filepath.Abs(".")
				require.NoError(t, err)
				newDir := filepath.Join(absolutePath, "newFolder")
				checkGeneratedFiles(fs, t, newDir)
			},
		}, {
			name: "should create the project at the given path, also" +
				"if the folder exists",
			args:	[]string{"generate", "--dir", "./existFolder"},
			before: func(fs afero.Fs) error {
				return fs.MkdirAll("./existFolder", 0755)
			},
			check: func(fs afero.Fs, t *testing.T) {
				absolutePath, err := filepath.Abs(".")
				require.NoError(t, err)
				newDir := filepath.Join(absolutePath, "existFolder")
				checkGeneratedFiles(fs, t, newDir)
			},
		}, {
			name: "should fail if the given dir contains files created by " +
				"this command*",
			args:	[]string{"generate", "--dir", "./existFolder"},
			before: func(fs afero.Fs) error {
				absolutePath, err := filepath.Abs(".")
				folderPath := filepath.Join(absolutePath, "existFolder")
				err = fs.MkdirAll(folderPath, 0755)
				_, err = fs.Create(filepath.Join(folderPath, "package.json"))
				return err
			},
			expectedErrMsg: fmt.Sprintf("The directory %s/existFolder/"+
				" contains files that could conflict: \n package.json", path),
		}, {
			name:	"should create webpack file with executable permission",
			args:	[]string{"generate"},
			check: func(fs afero.Fs, t *testing.T) {
				dir := filepath.Join(path, "wasm", "webpack.js")
				info, _ := fs.Stat(dir)
				require.True(t, info.Mode() == 0766)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := afero.NewMemMapFs()
			if tt.before != nil {
				err := tt.before(fs)
				require.NoError(t, err)
			}
			cmd := NewGenerateCommand(fs)
			cmd.SetArgs(tt.args)
			err = cmd.Execute()
			if tt.expectedErrMsg != "" {
				require.EqualError(t, err, tt.expectedErrMsg)
				return
			}
			if tt.check != nil {
				tt.check(fs, t)
			}
		})
	}
}
