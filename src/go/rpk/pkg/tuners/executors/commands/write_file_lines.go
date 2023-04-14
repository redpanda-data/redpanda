// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package commands

import (
	"bufio"
	"fmt"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/utils"
	"github.com/spf13/afero"
	"go.uber.org/zap"
)

type writeFileLinesCommand struct {
	Command
	fs    afero.Fs
	path  string
	lines []string
}

func NewWriteFileLinesCmd(fs afero.Fs, path string, lines []string) Command {
	return &writeFileLinesCommand{
		fs:    fs,
		path:  path,
		lines: lines,
	}
}

func (c *writeFileLinesCommand) Execute() error {
	zap.L().Sugar().Debugf("Writing '%v' to file '%s'", c.lines, c.path)
	return utils.WriteFileLines(c.fs, c.lines, c.path)
}

func (c *writeFileLinesCommand) RenderScript(w *bufio.Writer) error {
	fmt.Fprintf(w, "cat << EOF > %s\n", c.path)
	for _, line := range c.lines {
		fmt.Fprint(w, "  ")
		fmt.Fprintln(w, line)
	}
	fmt.Fprintln(w, "EOF")
	return w.Flush()
}
