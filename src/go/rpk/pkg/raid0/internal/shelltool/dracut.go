// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

//go:build fedora

package shelltool

type dracut struct {
	shelltool
}

func Dracut() *dracut {
	d := new(dracut)
	d.command = "/usr/bin/dracut"

	return d
}

func (d *dracut) RegenerateAll() *dracut {
	d.options = append(d.options, "--regenerate-all")
	return d
}

func (d *dracut) Force() *dracut {
	d.options = append(d.options, "--force")
	return d
}
