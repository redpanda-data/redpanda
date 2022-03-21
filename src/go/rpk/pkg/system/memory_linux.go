// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package system

import (
	"syscall"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
)

func getMemInfo(fs afero.Fs) (*MemInfo, error) {
	var si syscall.Sysinfo_t
	err := syscall.Sysinfo(&si)
	if err != nil {
		return nil, err
	}
	cGroupMemLimit, err := ReadCgroupMemLimitBytes(fs)
	if err != nil {
		// Systems such as Raspberry Pi do not support cgroups by default
		// unless /boot/cmdline.txt is modified, see:
		//
		//     https://downey.io/blog/exploring-cgroups-raspberry-pi/
		//
		// We do not need the cgroup memory limit, so this  error is
		// non-fatal.
		log.Debugf("Unable to query memory -- cgroups is likely not supported; err: %v", err)
	}
	return &MemInfo{
		MemTotal:  si.Totalram * uint64(si.Unit),
		MemFree:   si.Freeram * uint64(si.Unit),
		SwapTotal: si.Totalswap * uint64(si.Unit),

		CGroupMemLimit: cGroupMemLimit, // optional field last
	}, nil
}
