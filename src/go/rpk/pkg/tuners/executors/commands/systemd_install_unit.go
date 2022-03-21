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

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/system/systemd"
	"github.com/spf13/afero"
)

type installSystemdUnitCommand struct {
	client systemd.Client
	fs     afero.Fs
	body   string
	name   string
}

func NewInstallSystemdUnitCmd(
	client systemd.Client, fs afero.Fs, body,
	name string,
) (Command, error) {
	cmd := &installSystemdUnitCommand{
		client: client,
		fs:     fs,
		body:   body,
		name:   name,
	}
	return cmd, nil
}

func (cmd *installSystemdUnitCommand) Execute() error {
	return cmd.client.LoadUnit(cmd.fs, cmd.body, cmd.name)
}

func (cmd *installSystemdUnitCommand) RenderScript(w *bufio.Writer) error {
	catTmpl := `cat << EOF > %s
%s
EOF
`
	_, err := fmt.Fprintf(w, catTmpl, systemd.UnitPath(cmd.name), cmd.body)
	if err != nil {
		return err
	}
	_, err = fmt.Fprint(w, "sudo systemctl daemon-reload\n")
	return err
}
