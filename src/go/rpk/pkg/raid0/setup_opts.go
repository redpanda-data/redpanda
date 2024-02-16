// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package raid0

// RAIDOpt implements functional options for [Setup] as described in
// https://dave.cheney.net/2014/10/17/functional-options-for-friendly-apis
type RAIDOpt func(*raidOpts)

type raidOpts struct {
	devicePath        string
	mountPath         string
	blockSizeB        uint16
	chunkSizeKiB      uint32 // Same as strip depth in SNIA defined terms.
	memberDevicePaths []string
	mountOptions      string
	mountOnBoot       bool
}

// DevicePath sets the block device node for the RAID. Defaults to /dev/md0.
func DevicePath(dpath string) RAIDOpt {
	return func(o *raidOpts) {
		o.devicePath = dpath
	}
}

// MountPath sets the mount filesytem path or folder for the RAID device.
// Defaults to /var/lib/redpanda.
func MountPath(mpath string) RAIDOpt {
	return func(o *raidOpts) {
		o.mountPath = mpath
	}
}

// BlockSizeBytes sets the block size to pass to mkfs.xfs when formating the
// RAID device. Defaults to 4KiB.
func BlockSizeBytes(b uint16) RAIDOpt {
	return func(o *raidOpts) {
		o.blockSizeB = b
	}
}

// ChunkSizeKiB sets the RAID chunk size or strip depth. Defaults to 16KiB.
func ChunkSizeKiB(kib uint32) RAIDOpt {
	return func(o *raidOpts) {
		o.chunkSizeKiB = kib
	}
}

// MemberDevicePaths sets the devices that are going to be form the RAID 0
// device.
func MemberDevicePaths(p []string) RAIDOpt {
	return func(o *raidOpts) {
		o.memberDevicePaths = p
	}
}

// MountOptions are the mount options passed when mounting the XFS-formatted RAID
// device. Defaults to "defaults,noatime,nodiratime".
func MountOptions(mo string) RAIDOpt {
	return func(o *raidOpts) {
		o.mountOptions = mo
	}
}

// DisableMountOnBoot disables mounting the RAID device on boot.
func DisableMountOnBoot() RAIDOpt {
	return func(o *raidOpts) {
		o.mountOnBoot = false
	}
}
