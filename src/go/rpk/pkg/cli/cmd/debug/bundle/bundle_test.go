// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

//go:build linux
// +build linux

package bundle

import (
	"io"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

func TestLimitedWriter(t *testing.T) {
	const block = 4096
	tests := []struct {
		name          string
		limit         int
		blocksToWrite int
	}{{
		name:          "it should write everything if the limit is larger than the total bytes",
		limit:         3 * block,
		blocksToWrite: 2,
	}, {
		name:          "it should write up to the limit if the total bytes are larger than the limit",
		limit:         block,
		blocksToWrite: 2,
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(st *testing.T) {
			lim := &limitedWriter{
				w:          io.Discard,
				limitBytes: tt.limit,
			}
			var writeErr error
			remaining := tt.blocksToWrite
			written := 0
			for remaining > 0 {
				bs := make([]byte, int(block))

				var n int
				n, writeErr = lim.Write(bs)
				written += n
				if writeErr != nil {
					break
				}
				remaining--
			}
			var expected int
			totalBytes := tt.blocksToWrite * block
			if totalBytes > tt.limit {
				require.EqualError(st, writeErr, "output size limit reached")
				expected = tt.limit
			} else if totalBytes <= tt.limit {
				require.NoError(st, writeErr)
				expected = totalBytes
			}
			require.Equal(st, expected, written)
		})
	}
}

func TestWalkDirMissingRoot(t *testing.T) {
	files := make(map[string]*fileInfo)
	root := "/etc/its_highly_unlikely_that_a_dir_named_like_this_exists_anywhere"
	err := walkDir(root, files)

	require.NoError(t, err)

	// The actual output differs from OS to OS, so to prevent a flaky test, just check that the file
	// was added to the files map, and that it has an associated error related to the filename.
	require.Contains(t, files[root].Error, "/etc/its_highly_unlikely_that_a_dir_named_like_this_exists_anywhere")
}

func TestDetermineFilepath(t *testing.T) {
	for _, test := range []struct {
		name     string
		filepath string
		exp      string
		expErr   bool
	}{
		{"empty filepath", "", "-bundle.zip", false},
		{"correct filepath", "/tmp/customDebugName.zip", "/tmp/customDebugName.zip", false},
		{"filepath with no extension", "/tmp/file", "/tmp/file.zip", false},
		{"filepath is a directory", "/tmp", "", true},
		{"unsupported extension", "customDebugName.tar.gz", "", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			fs := afero.NewMemMapFs()
			// create /tmp folder for the test cases.
			err := fs.Mkdir("/tmp", 0o755)
			require.NoError(t, err)

			filepath, err := determineFilepath(fs, test.filepath, false)
			if test.expErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			if test.filepath != "" {
				require.Equal(t, test.exp, filepath)
				return
			}
			require.Contains(t, filepath, test.exp)
		})
	}
}
