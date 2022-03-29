// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package filesystem

import (
	"syscall"

	"golang.org/x/sys/unix"
)

func GetFilesystemType(path string) (FsType, error) {
	statFs := syscall.Statfs_t{}
	err := syscall.Statfs(path, &statFs)
	if err != nil {
		return "", err
	}
	switch statFs.Type {
	case unix.EXT4_SUPER_MAGIC:
		return Ext, nil
	case unix.XFS_SUPER_MAGIC:
		return Xfs, nil
	case unix.TMPFS_MAGIC:
		return Tmpfs, nil
	case 0x4244:
		return Hfs, nil
	default:
		return Unknown, nil
	}
}
