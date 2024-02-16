// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package shelltool

type systemdEscape struct {
	shelltool
}

// SystemdEscape escape strings for usage in systemd unit names.
func SystemdEscape(unitName string) *systemdEscape {
	s := new(systemdEscape)
	s.command = "/usr/bin/systemd-escape"
	s.arguments = append(s.arguments, unitName)

	return s
}

// Path makes systemd-escape assume a path when escaping/unescaping.
func (s *systemdEscape) Path() *systemdEscape {
	s.options = append(s.options, "--path")
	return s
}

// Suffix sets the systemd unit suffix to append to the escaped string.
func (s *systemdEscape) Suffix(v string) *systemdEscape {
	s.options = append(s.options, "--suffix", v)
	return s
}
