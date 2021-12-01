// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package status

import (
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/api/admin"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/config"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/out"
)

func NewCommand(fs afero.Fs) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Get configuration status of redpanda nodes",
		Long: `Get configuration status of redpanda nodes

For each node, indicate which configuration version it is up to date with,
whether it requires a restart for settings to take effect, and any invalid
or unknown properties.
`,
		Run: func(cmd *cobra.Command, args []string) {
			p := config.ParamsFromCommand(cmd)
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			client, err := admin.NewClient(fs, cfg)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			// GET the status endpoint
			resp, err := client.ClusterConfigStatus()
			out.MaybeDie(err, "Error fetching status: %v", err)

			tw := out.NewTable("NODE", "CONFIG_VERSION", "NEEDS_RESTART", "INVALID", "UNKNOWN")
			defer tw.Flush()

			for _, node := range resp {
				tw.Print(node.NodeId, node.ConfigVersion, node.Restart, node.Invalid, node.Unknown)
			}
		},
	}

	return cmd
}
