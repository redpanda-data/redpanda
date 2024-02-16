// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package shelltool

type initRAMFS struct {
	shelltool
}

func InitRAMFS() *initRAMFS {
	i := new(initRAMFS)
	i.command = "/usr/sbin/update-initramfs"
	return i
}

func (i *initRAMFS) Update() *initRAMFS {
	i.options = append(i.options, "-u")
	return i
}
