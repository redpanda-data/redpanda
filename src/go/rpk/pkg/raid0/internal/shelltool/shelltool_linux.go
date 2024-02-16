// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

//go:build linux

package shelltool

import "golang.org/x/sys/unix"

// In case there are commands spawning subcommands, we need to signal them
// to gracefully terminate when the parent command is terminated or killed.
var defaultSysProcAttr = &unix.SysProcAttr{
	// SIGTERM children if parent thread is dead. Requires locking
	// goroutine execution to the underlying OS thread using
	// [runtime.LockOSThread].
	Pdeathsig: unix.SIGTERM,
	// set process group ID
	Setpgid: true,
}
