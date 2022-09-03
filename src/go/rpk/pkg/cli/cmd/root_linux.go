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
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cmd/iotune"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cmd/redpanda"
	rp "github.com/redpanda-data/redpanda/src/go/rpk/pkg/redpanda"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func addPlatformDependentCmds(fs afero.Fs, cmd *cobra.Command) {
	cmd.AddCommand(redpanda.NewCommand(fs, rp.NewLauncher()))
	cmd.AddCommand(iotune.NewCommand(fs))

	// deprecated
	cmd.AddCommand(NewCheckCommand(fs))
	cmd.AddCommand(NewConfigCommand(fs))
	cmd.AddCommand(NewModeCommand(fs))
	cmd.AddCommand(NewStartCommand(fs, rp.NewLauncher()))
	cmd.AddCommand(NewStatusCommand())
	cmd.AddCommand(NewStopCommand(fs))
	cmd.AddCommand(NewTuneCommand(fs))
}
