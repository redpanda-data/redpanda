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
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/iotune"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/redpanda"
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
		NewCheckCommand(fs, p),
		NewConfigCommand(fs, p),
		NewModeCommand(fs, p),
		NewStartCommand(fs, p, rp.NewLauncher()),
		NewStatusCommand(),
		NewStopCommand(fs, p),
		NewTuneCommand(fs, p),
	)
}
