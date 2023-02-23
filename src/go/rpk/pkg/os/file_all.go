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
