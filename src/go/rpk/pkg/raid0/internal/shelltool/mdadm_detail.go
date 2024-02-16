// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package shelltool

type mdadmDetail struct {
	shelltool
}

// MdadmCreate assembles and creates Linux md devices (aka RAID arrays).
func MdadmDetail() *mdadmDetail {
	m := new(mdadmDetail)
	m.command = "/usr/sbin/mdadm"
	m.options = append(m.options, "--detail")

	return m
}

func (m *mdadmDetail) Scan() *mdadmDetail {
	m.options = append(m.options, "--scan")
	return m
}
