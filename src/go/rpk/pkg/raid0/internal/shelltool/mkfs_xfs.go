// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package shelltool

import (
	"fmt"
)

type makeXFS struct {
	shelltool
}

func MakeXFS(device string) *makeXFS {
	m := new(makeXFS)
	m.command = "/usr/sbin/mkfs.xfs"
	m.arguments = append(m.arguments, device)

	return m
}

// BlockSize in bytes
func (m *makeXFS) BlockSize(bs uint16) *makeXFS {
	m.options = append(m.options, "-b", fmt.Sprintf("size=%d", bs))
	return m
}

// NoDiscard skips discarding blocks, to speed up formatting.
func (m *makeXFS) NoDiscard() *makeXFS {
	m.options = append(m.options, "-K")
	return m
}
