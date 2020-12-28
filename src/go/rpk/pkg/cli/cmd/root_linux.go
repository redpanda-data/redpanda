// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package cmd

import (
	"vectorized/pkg/config"
	"vectorized/pkg/redpanda"

	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func addPlatformDependentCmds(
	fs afero.Fs, mgr config.Manager, cmd *cobra.Command,
) {
	cmd.AddCommand(NewTuneCommand(fs, mgr))
	cmd.AddCommand(NewCheckCommand(fs, mgr))
	cmd.AddCommand(NewIoTuneCmd(fs, mgr))
	cmd.AddCommand(NewStartCommand(fs, mgr, redpanda.NewLauncher()))
	cmd.AddCommand(NewStopCommand(fs, mgr))
}
