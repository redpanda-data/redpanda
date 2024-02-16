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
	"context"
	"os/exec"

	"golang.org/x/sys/unix"
)

type shelltool struct {
	command   string
	options   []string
	arguments []string
	error     error
}

// Build builds the full command and returns an *[exec.Cmd] instance, or an [error].
// By default, it configures [cmd.SysProcAttr] and [cmd.Cancel] to gracefully
// SIGTERM children processes, however, it also requires the caller of this function
// to execute the command in the same OS thread using [runtime.LockOSThread]
// due to https://github.com/golang/go/issues/27505.
func (s *shelltool) Build(ctx context.Context) (*exec.Cmd, error) {
	args := append(s.options, s.arguments...)
	cmd := exec.CommandContext(ctx, s.command, args...)
	cmd.SysProcAttr = defaultSysProcAttr
	cmd.Cancel = func() error {
		// Attempt to gracefully terminate command and its subcommands on
		// context cancellation. Go's default behavior is to SIGKILL the
		// subprocesses.
		return cmd.Process.Signal(unix.SIGTERM)
	}
	return cmd, s.error
}
