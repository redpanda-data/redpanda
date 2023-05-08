// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package cli

import (
	"fmt"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/debug"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/iotune"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/redpanda"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/redpanda/tune"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	rp "github.com/redpanda-data/redpanda/src/go/rpk/pkg/redpanda"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func addPlatformDependentCmds(fs afero.Fs, p *config.Params, cmd *cobra.Command) {
	cmd.AddCommand(
		redpanda.NewCommand(fs, p, rp.NewLauncher()),
		iotune.NewCommand(fs, p),
	)

	// deprecated
	cmd.AddCommand(
		newCheckCommand(fs, p),
		newConfigCommand(fs, p),
		newModeCommand(fs, p),
		newStartCommand(fs, p, rp.NewLauncher()),
		newStatusCommand(),
		newStopCommand(fs, p),
		newTuneCommand(fs, p),
	)
}

func deprecateCmd(newCmd *cobra.Command, newUse string) *cobra.Command {
	newCmd.Deprecated = fmt.Sprintf("use %q instead", newUse)
	newCmd.Hidden = true
	if children := newCmd.Commands(); len(children) > 0 {
		for _, child := range children {
			deprecateCmd(child, newUse+" "+child.Name())
		}
	}
	return newCmd
}

func newCheckCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	return deprecateCmd(redpanda.NewCheckCommand(fs, p), "rpk redpanda check")
}

func newConfigCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	return deprecateCmd(redpanda.NewConfigCommand(fs, p), "rpk redpanda config")
}

func newModeCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	return deprecateCmd(redpanda.NewModeCommand(fs, p), "rpk redpanda mode")
}

func newStartCommand(fs afero.Fs, p *config.Params, launcher rp.Launcher) *cobra.Command {
	return deprecateCmd(redpanda.NewStartCommand(fs, p, launcher), "rpk redpanda start")
}

func newStatusCommand() *cobra.Command {
	return deprecateCmd(debug.NewInfoCommand(), "rpk debug info")
}

func newStopCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	return deprecateCmd(redpanda.NewStopCommand(fs, p), "rpk redpanda stop")
}

func newTuneCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	return deprecateCmd(tune.NewCommand(fs, p), "rpk redpanda tune")
}
