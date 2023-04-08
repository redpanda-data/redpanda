// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package disk

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/afero"
	"go.uber.org/zap"
	"golang.org/x/sys/unix"
)

func NewDevice(dev uint64, fs afero.Fs) (BlockDevice, error) {
	maj := unix.Major(dev)
	min := unix.Minor(dev)
	zap.L().Sugar().Debugf("Creating block device from number {%d, %d}", maj, min)
	syspath, err := readSyspath(maj, min)
	if err != nil {
		return nil, err
	}
	return deviceFromSystemPath(syspath, fs)
}

func readSyspath(major, minor uint32) (string, error) {
	blockBasePath := "/sys/dev/block"
	path := fmt.Sprintf("%s/%d:%d", blockBasePath, major, minor)
	linkpath, err := os.Readlink(path)
	if err != nil {
		return "", err
	}
	return filepath.Abs(filepath.Join(blockBasePath, linkpath))
}
