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
	"time"

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

func TestParseJournalTime(t *testing.T) {
	for _, test := range []struct {
		name   string
		inStr  string
		inTime time.Time
		exp    time.Time
		expErr bool
	}{
		{
			name:  "YYYY-MM-DD",
			inStr: "2023-11-30",
			exp:   time.Date(2023, 11, 30, 0, 0, 0, 0, time.Local),
		}, {
			name:  "YYYY-MM-DD with leading and trailing space",
			inStr: " 2023-11-30 ",
			exp:   time.Date(2023, 11, 30, 0, 0, 0, 0, time.Local),
		}, {
			name:  "YYYY-MM-DD HH:MM:SS",
			inStr: "2023-11-30 12:13:14",
			exp:   time.Date(2023, 11, 30, 12, 13, 14, 0, time.Local),
		}, {
			name:  "No seconds: YYYY-MM-DD HH:MM",
			inStr: "2023-11-30 12:13",
			exp:   time.Date(2023, 11, 30, 12, 13, 0, 0, time.Local),
		}, {
			name:   "YYYY-MM-DD HH No Minutes",
			inStr:  "2023-11-30 12",
			expErr: true, // This is not specified in man journalctl, so we err.
		}, {
			name:   "YYYY-MM-DD HH:MM:S With 1 second",
			inStr:  "2023-11-30 12:13:1",
			expErr: true, // This is not specified in man journalctl, so we err.
		}, {
			name:   "YYYY-MM-DD HH:MM: with extra :",
			inStr:  "2023-11-30 12:13:",
			expErr: true, // This is an error in journalctl too.
		}, {
			name:   "now",
			inStr:  "now",
			inTime: time.Date(2022, time.November, 8, 22, 15, 0, 0, time.Local),
			exp:    time.Date(2022, time.November, 8, 22, 15, 0, 0, time.Local),
		}, {
			name:   "today",
			inStr:  "today",
			inTime: time.Date(2023, time.January, 18, 10, 50, 0, 0, time.Local),
			exp:    time.Date(2023, time.January, 18, 0, 0, 0, 0, time.Local),
		}, {
			name:   "yesterday",
			inStr:  "yesterday",
			inTime: time.Date(2023, time.January, 0o1, 15, 45, 12, 0, time.Local),
			exp:    time.Date(2022, time.December, 31, 0, 0, 0, 0, time.Local),
		}, {
			name:   "unrecognized text",
			inStr:  "todayzz",
			expErr: true,
		}, {
			name:   "+ relative time",
			inStr:  "2h",
			inTime: time.Date(2022, time.November, 8, 22, 15, 0, 0, time.Local),
			exp:    time.Date(2022, time.November, 9, 0o0, 15, 0, 0, time.Local),
		}, {
			name:   "- relative time",
			inStr:  "-48h",
			inTime: time.Date(2022, time.February, 18, 8, 0, 0, 0, time.Local),
			exp:    time.Date(2022, time.February, 16, 8, 0, 0, 0, time.Local),
		}, {
			name:   "unrecognized relative time",
			inStr:  "-5trillions",
			expErr: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			parsedTime, err := parseJournalTime(test.inStr, test.inTime)
			if test.expErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			require.Equal(t, test.exp, parsedTime)
		})
	}
}

func TestSortControllerLogDir(t *testing.T) {
	for _, test := range []struct {
		name string
		in   []fileSize
		exp  []fileSize
	}{
		{
			name: "already sorted",
			in: []fileSize{
				{"/tmp/1-1-v1.log", 111},
				{"/tmp/1-2-v1.log", 111},
				{"/tmp/2-1-v1.log", 111},
				{"/tmp/3-2-v1.log", 111},
			},
			exp: []fileSize{
				{"/tmp/1-1-v1.log", 111},
				{"/tmp/1-2-v1.log", 111},
				{"/tmp/2-1-v1.log", 111},
				{"/tmp/3-2-v1.log", 111},
			},
		}, {
			name: "sort correctly",
			in: []fileSize{
				{"/tmp/12-1-v1.log", 111},
				{"/tmp/1-21-v1.log", 111},
				{"/tmp/23-1-v1.log", 111},
				{"/tmp/2-31-v1.log", 111},
				{"/tmp/5000-31-v1.log", 111},
			},
			exp: []fileSize{
				{"/tmp/1-21-v1.log", 111},
				{"/tmp/2-31-v1.log", 111},
				{"/tmp/12-1-v1.log", 111},
				{"/tmp/23-1-v1.log", 111},
				{"/tmp/5000-31-v1.log", 111},
			},
		}, {
			name: "bad names are sorted alphabetically",
			in: []fileSize{
				{"/tmp/what.log", 111},
				{"/tmp/how?", 111},
				{"/tmp/seems-legit.log", 111},
			},
			exp: []fileSize{
				{"/tmp/how?", 111},
				{"/tmp/seems-legit.log", 111},
				{"/tmp/what.log", 111},
			},
		}, {
			name: "corrupted names sorted at the top",
			in: []fileSize{
				{"/tmp/A", 111},
				{"/tmp/12-1-v1.log", 111},
				{"/tmp/B.log", 111},
				{"/tmp/2-31-v1.log", 111},
				{"/tmp/seems-legit.log", 111},
				{"/tmp/1-21-v1.log", 111},
				{"/tmp/5000-31-v1.log", 111},
				{"/tmp/23-1-v1.log", 111},
			},
			exp: []fileSize{
				{"/tmp/A", 111},
				{"/tmp/B.log", 111},
				{"/tmp/seems-legit.log", 111},
				{"/tmp/1-21-v1.log", 111},
				{"/tmp/2-31-v1.log", 111},
				{"/tmp/12-1-v1.log", 111},
				{"/tmp/23-1-v1.log", 111},
				{"/tmp/5000-31-v1.log", 111},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			sortControllerLogDir(test.in)
			require.Equal(t, test.exp, test.in)
		})
	}
}

func TestSliceControllerDir(t *testing.T) {
	for _, test := range []struct {
		name  string
		in    []fileSize
		limit int64
		exp   []fileSize
	}{
		{
			name: "simple slice",
			in: []fileSize{
				{"/tmp/1-1-v1.log", 1},
				{"/tmp/2-1-v1.log", 1},
				{"/tmp/3-1-v1.log", 1},
				{"/tmp/4-1-v1.log", 1},
				{"/tmp/5-1-v1.log", 1},
				{"/tmp/6-1-v1.log", 1},
			},
			limit: 4,
			exp: []fileSize{
				{"/tmp/1-1-v1.log", 1},
				{"/tmp/2-1-v1.log", 1},
				{"/tmp/6-1-v1.log", 1}, // we don't preserve the order, but it's fine since we don't need the logs to be sorted to debug them.
				{"/tmp/5-1-v1.log", 1},
			},
		}, {
			name: "big file in head",
			in: []fileSize{
				{"/tmp/1-1-v1.log", 10000},
				{"/tmp/2-1-v1.log", 1},
				{"/tmp/3-1-v1.log", 1},
				{"/tmp/4-1-v1.log", 1},
				{"/tmp/5-1-v1.log", 1},
				{"/tmp/6-1-v1.log", 1},
			},
			limit: 4,
			exp: []fileSize{
				{"/tmp/6-1-v1.log", 1},
				{"/tmp/5-1-v1.log", 1},
				{"/tmp/4-1-v1.log", 1},
				{"/tmp/3-1-v1.log", 1},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			slice := sliceControllerDir(test.in, test.limit)
			require.Equal(t, test.exp, slice)
		})
	}
}
