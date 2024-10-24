// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

//go:build linux
// +build linux

package commands

import (
	"bufio"
	"fmt"
	"os"

	"go.uber.org/zap"
	"golang.org/x/sys/unix"
)

type writeSizedFileCommand struct {
	path      string
	sizeBytes int64
}

// NewWriteSizedFileCmd creates a file of size sizeBytes at the given path.
// When executed through DirectExecutor, it uses fallocate to create
// the file. If the file already existed, then it uses ftruncate to shrink it
// down if needed.
// The script rendered through a ScriptRenderingExecutor calls `truncate`,
// which has the same behavior.
func NewWriteSizedFileCmd(path string, sizeBytes int64) Command {
	return &writeSizedFileCommand{path, sizeBytes}
}

func (c *writeSizedFileCommand) Execute() error {
	zap.L().Sugar().Debugf("Creating '%s' (%d B)", c.path, c.sizeBytes)

	// the 'os' package needs to be used instead of 'afero', because the file
	// handles returned by afero don't have a way to get their file descriptor
	// (i.e. an Fd() method):
	// https://github.com/spf13/afero/issues/234
	f, err := os.OpenFile(c.path, os.O_WRONLY|os.O_CREATE, 0o666)
	if err != nil {
		return err
	}
	defer f.Close()

	err = unix.Fallocate(int(f.Fd()), 0, 0, c.sizeBytes)
	if err != nil {
		return fmt.Errorf("could not allocate the requested size while"+
			" creating the file at '%s': %w",
			c.path,
			err,
		)
	}

	// If the file already exists, then its current size might be larger than desired.
	// Use ftruncate to ensure that doesn't happen.
	err = unix.Ftruncate(int(f.Fd()), c.sizeBytes)
	if err != nil {
		return fmt.Errorf("could not resize the file at '%s' to the requested size: %w",
			c.path,
			err,
		)
	}
	err = f.Sync()
	if err != nil {
		return fmt.Errorf("unable to sync the file at %s: %w", c.path, err)
	}
	return nil
}

func (c *writeSizedFileCommand) RenderScript(w *bufio.Writer) error {
	// See 'man truncate'.
	fmt.Fprintf(
		w,
		"truncate -s %d %s\n",
		c.sizeBytes,
		c.path,
	)
	return w.Flush()
}
