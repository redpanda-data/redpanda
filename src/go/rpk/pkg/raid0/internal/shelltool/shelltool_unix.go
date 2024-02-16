// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// list taken from https://github.com/golang/go/blob/91ef076562dfcf783074dbd84ad7c6db60fdd481/src/go/build/syslist.go#L38-L51
//go:build aix || darwin || dragonfly || freebsd || hurd || illumos || ios || netbsd || openbsd || solaris

package shelltool

import "golang.org/x/sys/unix"

var defaultSysProcAttr = &unix.SysProcAttr{}
