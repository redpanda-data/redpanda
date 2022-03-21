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
	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"golang.org/x/sys/unix"
)

func NewDevice(dev uint64, fs afero.Fs) (BlockDevice, error) {
	maj := unix.Major(dev)
	min := unix.Minor(dev)
	log.Debugf("Creating block device from number {%d, %d}", maj, min)
	syspath, err := readSyspath(maj, min)
	if err != nil {
		return nil, err
	}
	return deviceFromSystemPath(syspath, fs)
}
