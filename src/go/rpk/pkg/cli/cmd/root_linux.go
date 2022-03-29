// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package cmd

import (
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/redpanda"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func addPlatformDependentCmds(
	fs afero.Fs, mgr config.Manager, cmd *cobra.Command,
) {
	cmd.AddCommand(NewRedpandaCommand(fs, mgr, redpanda.NewLauncher()))
	cmd.AddCommand(NewDebugCommand(fs))

	cmd.AddCommand(NewTuneCommand(fs, mgr))
	cmd.AddCommand(NewCheckCommand(fs, mgr))
	cmd.AddCommand(NewIoTuneCmd(fs, mgr))
	cmd.AddCommand(NewStartCommand(fs, mgr, redpanda.NewLauncher()))
	cmd.AddCommand(NewStopCommand(fs, mgr))
	cmd.AddCommand(NewConfigCommand(fs, mgr))
	cmd.AddCommand(NewStatusCommand(fs, mgr))
	cmd.AddCommand(NewModeCommand(mgr))
}
