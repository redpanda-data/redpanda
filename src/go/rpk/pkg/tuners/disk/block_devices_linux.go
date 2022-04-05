// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package disk

import "syscall"

func getDevNumFromDeviceDirectory(stat syscall.Stat_t) uint64 {
	return stat.Rdev
}

func getDevNumFromDirectory(stat syscall.Stat_t) uint64 {
	return stat.Dev
}
