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

package syslog

import (
	"fmt"

	"golang.org/x/sys/unix"
)

func ReadAll() ([]byte, error) {
	// Get the syslog buffer size. See 'man 2 syslog'
	bufSize, err := unix.Klogctl(unix.SYSLOG_ACTION_SIZE_BUFFER, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get the size of the kernel log buffer: %w", err)
	}

	buf := make([]byte, bufSize)

	read, err := unix.Klogctl(unix.SYSLOG_ACTION_READ_ALL, buf)
	if err != nil {
		return nil, fmt.Errorf("failed to read kernel logs: %w", err)
	}

	return buf[:read], nil
}
