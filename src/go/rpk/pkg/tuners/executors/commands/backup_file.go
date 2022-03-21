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
	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
)

type backupFileCommand struct {
	Command
	fs   afero.Fs
	path string
}

func NewBackupFileCmd(fs afero.Fs, path string) Command {
	return &backupFileCommand{
		fs:   fs,
		path: path,
	}
}

func (c *backupFileCommand) Execute() error {
	log.Debugf("Creating backup of '%s'", c.path)
	bckFile, err := utils.BackupFile(c.fs, c.path)
	if err == nil {
		log.Debugf("Backup created '%s'", bckFile)
	}
	return err
}

func (c *backupFileCommand) RenderScript(w *bufio.Writer) error {
	fmt.Fprintf(w, "md_5=$(md5sum %s | awk '{print $1}')\n", c.path)
	fmt.Fprintf(w, "cp %s %s.vectorized.${md_5}.bk\n", c.path, c.path)
	return w.Flush()
}
