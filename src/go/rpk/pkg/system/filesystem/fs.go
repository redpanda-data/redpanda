// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

//go:build !windows

package filesystem

import (
	"path/filepath"
	"syscall"

	"github.com/docker/go-units"
	"github.com/spf13/afero"
)

func DirectoryIsWriteable(fs afero.Fs, path string) (bool, error) {
	if exists, _ := afero.Exists(fs, path); !exists {
		err := fs.MkdirAll(path, 0o755)
		if err != nil {
			return false, err
		}
	}
	testFile := filepath.Join(path, "test_file")
	err := afero.WriteFile(fs, testFile, []byte{0}, 0o644)
	if err != nil {
		return false, err
	}
	err = fs.Remove(testFile)
	if err != nil {
		return false, err
	}

	return true, nil
}

func GetFreeDiskSpaceGB(path string) (float64, error) {
	statFs := syscall.Statfs_t{}
	err := syscall.Statfs(path, &statFs)
	if err != nil {
		return 0, err
	}
	return float64(statFs.Bfree*uint64(statFs.Bsize)) / units.GiB, nil
}
