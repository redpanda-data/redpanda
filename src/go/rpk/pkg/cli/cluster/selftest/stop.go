// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package selftest

import (
	"fmt"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"

	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newStopCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	return &cobra.Command{
		Use:   "stop",
		Short: "Stops the currently executing self-test",
		Long: `Stops all self-test tests.

This command stops all currently running self-tests. The command is synchronous and returns
success when all jobs have been stopped or reports errors if broker timeouts have expired.
`,
		Args: cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, _ []string) {
			// Load config settings
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			config.CheckExitCloudAdmin(p)

			// Create new HTTP client for communication w/ admin server
			cl, err := adminapi.NewClient(cmd.Context(), fs, p)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			// Make HTTP POST request to leader that stops all self tests on all nodes
			err = cl.StopSelfTest(cmd.Context())
			out.MaybeDie(err, "unable to stop self test: %v", err)

			fmt.Print("All self-test jobs have been stopped\n")
		},
	}
}
