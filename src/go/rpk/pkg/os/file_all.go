// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

//go:build !windows

package os

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"syscall"

	"github.com/spf13/afero"
)

// PreserveUnixOwnership chowns a file to the perms in stat.
func PreserveUnixOwnership(fs afero.Fs, stat os.FileInfo, file string) error {
	// Stat_t is only valid in unix not on Windows.
	if stat, ok := stat.Sys().(*syscall.Stat_t); ok {
		gid := int(stat.Gid)
		uid := int(stat.Uid)
		err := fs.Chown(file, uid, gid)
		if err != nil {
			return fmt.Errorf("unable to chown temp config file: %v", err)
		}
	}
	return nil
}

// DirSize returns the size of a directory. It will exclude the size of the
// files that match the 'exclude' regexp.
func DirSize(path string, exclude *regexp.Regexp) (int64, error) {
	var size int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			if exclude != nil {
				if !exclude.MatchString(info.Name()) {
					size += info.Size()
				}
			} else {
				size += info.Size()
			}
		}
		return err
	})
	return size, err
}
