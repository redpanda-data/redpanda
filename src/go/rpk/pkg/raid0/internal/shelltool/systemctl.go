// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package shelltool

type systemctl struct {
	shelltool
	isSubCommandSet bool
}

func SystemCTL() *systemctl {
	s := new(systemctl)
	s.command = "/usr/bin/systemctl"

	return s
}

func (s *systemctl) Unit(name string) *systemctl {
	s.arguments = append(s.arguments, name)
	return s
}

// Enable configures the unit to be started on boot.
func (s *systemctl) Enable() *systemctl {
	if s.isSubCommandSet {
		return s
	}
	s.isSubCommandSet = true

	s.arguments = append(s.arguments, "enable")
	return s
}

// Start runs systemctl start on the unit.
func (s *systemctl) Start() *systemctl {
	if s.isSubCommandSet {
		return s
	}
	s.isSubCommandSet = true
	s.arguments = append(s.arguments, "start")
	return s
}

// Reload reloads systemd manager configuration.
// This will rerun all generators (see systemd.generator(7)),
// reload all unit files, and recreate the entire dependency tree.
// While the daemon is being reloaded, all sockets systemd listens
// on behalf of user configuration will stay accessible.
//
// This command should not be confused with the reload command.
// -- https://www.freedesktop.org/software/systemd/man/latest/systemctl.html#daemon-reload
func (s *systemctl) DaemonReload() *systemctl {
	if s.isSubCommandSet {
		return s
	}
	s.isSubCommandSet = true
	s.arguments = append(s.arguments, "daemon-reload")
	return s
}
