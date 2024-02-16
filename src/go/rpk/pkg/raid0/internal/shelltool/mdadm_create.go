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

type mdadmCreate struct {
	shelltool
}

// MdadmCreate assembles and creates Linux md devices (aka RAID arrays).
func MdadmCreate(device string) *mdadmCreate {
	m := new(mdadmCreate)
	m.command = "/usr/sbin/mdadm"
	m.options = append(m.options, "--create", device)

	return m
}

// Verbose increases output logging.
func (m *mdadmCreate) Verbose() *mdadmCreate {
	m.options = append(m.options, "--verbose")
	return m
}

// Force honours devices as given through [Devices].
func (m *mdadmCreate) Force() *mdadmCreate {
	m.options = append(m.options, "--force")
	return m
}

// Run insists of running the array even if not all devices are present or some
// look odd.
func (m *mdadmCreate) Run() *mdadmCreate {
	m.options = append(m.options, "--run")
	return m
}

// Level is the RAID level: 0,1,4,5,6,10,linear,multipath and synonyms.
func (m *mdadmCreate) Level(l string) *mdadmCreate {
	m.options = append(m.options, "--level", l)
	return m
}

// HomeHost overrides any HOMEHOST setting in the mdadm.conf file and provides
// the identity of the host which should be considered the home for any arrays.
// The  special name "any" can be used as a wild card.  If an array is created
// with "any" then the name "any" will be stored in the array and it can be
// assembled in the same way on any host. If an array is assembled with this option,
// then the homehost recorded on the array will be ignored.
func (m *mdadmCreate) HomeHost(h string) *mdadmCreate {
	m.options = append(m.options, "--homehost", h)
	return m
}

// ChuckSize is the RAID chunk size in kibibytes. A kibibyte is 1024 bytes.
func (m *mdadmCreate) ChunkSize(s int) *mdadmCreate {
	m.options = append(m.options, "--chunk", fmt.Sprintf("%d", s))
	return m
}

// DeviceNumber is the number of active devices in array.
func (m *mdadmCreate) DeviceNumber(n int) *mdadmCreate {
	m.options = append(m.options, "--raid-devices", fmt.Sprintf("%d", n))
	return m
}

func (m *mdadmCreate) Devices(d ...string) *mdadmCreate {
	m.arguments = append(m.arguments, d...)
	return m
}
