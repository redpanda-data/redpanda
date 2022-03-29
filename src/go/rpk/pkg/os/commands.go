// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package os

import "time"

type Commands interface {
	Which(cmd string, timeout time.Duration) (string, error)
}

func NewCommands(proc Proc) Commands {
	return &commands{proc}
}

type commands struct {
	proc Proc
}

func (commands *commands) Which(
	cmd string, timeout time.Duration,
) (string, error) {
	out, err := commands.proc.RunWithSystemLdPath(timeout, "which", cmd)
	if err == nil {
		return out[0], nil
	}
	return "", err
}
