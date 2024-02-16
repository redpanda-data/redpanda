// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package shelltool

type wipeFS struct {
	shelltool
}

// WipeFS removes filesystem signatures from a disk device.
func WipeFS(device string) *wipeFS {
	w := new(wipeFS)
	w.command = "/usr/sbin/wipefs"
	w.arguments = append(w.arguments, device)

	return w
}

// All deletes ALL filesystem signatures found.
func (w *wipeFS) All() *wipeFS {
	w.options = append(w.options, "--all")
	return w
}
