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
	"errors"

	"github.com/docker/go-units"
	"github.com/spf13/afero"
)

type MemInfo struct {
	MemTotal  uint64
	MemFree   uint64
	SwapTotal uint64

	// CGroupMemLimit does not load on all unix systems, specifically,
	// unless Raspberry Pi's /boot/cmdline.txt is modified, memory cgroups
	// will not be present.
	//
	// Its value will be either the cgroup's memory.limit_in_bytes, or 0.
	CGroupMemLimit uint64 `json:"omitempty"`
}

func GetTransparentHugePagesActive(fs afero.Fs) (bool, error) {
	options, err := ReadRuntineOptions(fs,
		"/sys/kernel/mm/transparent_hugepage/enabled")
	if err != nil {
		return false, err
	}

	if options.GetActive() != "never" {
		return true, nil
	}

	return false, nil
}

func GetMemTotalMB(fs afero.Fs) (int, error) {
	mInfo, err := getMemInfo(fs)
	if err != nil {
		return 0, err
	}

	if mInfo.CGroupMemLimit == 0 {
		return 0, errors.New("unable to determine cgroup memory limit")
	}
	memBytes := min(mInfo.MemTotal, mInfo.CGroupMemLimit)
	return int(memBytes / units.MiB), nil
}

func IsSwapEnabled(fs afero.Fs) (bool, error) {
	memInfo, err := getMemInfo(fs)
	if err != nil {
		return false, err
	}
	return memInfo.SwapTotal != 0, nil
}

func min(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
