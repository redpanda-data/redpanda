// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package config

import (
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/api/admin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newStatusCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Get configuration status of redpanda nodes.",
		Long: `Get configuration status of redpanda nodes.

For each node, indicate whether a restart is required for settings to
take effect, and any settings that the node has identified as invalid
or unknown properties.

Additionally show the version of cluster configuration that each node
has applied: under normal circumstances these should all be equal,
a lower number shows that a node is out of sync, perhaps because it
is offline.`,
		Run: func(cmd *cobra.Command, args []string) {
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			client, err := admin.NewClient(fs, cfg)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			// GET the status endpoint
			resp, err := client.ClusterConfigStatus(cmd.Context(), false)
			out.MaybeDie(err, "error fetching status: %v", err)

			tw := out.NewTable("NODE", "CONFIG-VERSION", "NEEDS-RESTART", "INVALID", "UNKNOWN")
			defer tw.Flush()

			for _, node := range resp {
				tw.PrintStructFields(struct {
					ID      int64
					Version int64
					Restart bool
					Invalid []string
					Unknown []string
				}{node.NodeID, node.ConfigVersion, node.Restart, node.Invalid, node.Unknown})
			}
		},
	}

	return cmd
}
