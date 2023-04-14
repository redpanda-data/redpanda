// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package cluster

import (
	"fmt"
	"reflect"
	"time"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/api/admin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newHealthOverviewCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var watch, exit bool
	cmd := &cobra.Command{
		Use:   "health",
		Short: "Queries cluster for health overview",
		Long: `Queries health overview.

Health overview is created based on the health reports collected periodically
from all nodes in the cluster. A cluster is considered healthy when the
following conditions are met:

* all cluster nodes are responding
* all partitions have leaders
* the cluster controller is present
`,
		Args: cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, _ []string) {
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			cl, err := admin.NewClient(fs, cfg)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			var lastOverview admin.ClusterHealthOverview
			for {
				ret, err := cl.GetHealthOverview(cmd.Context())
				out.MaybeDie(err, "unable to request cluster health: %v", err)
				if !reflect.DeepEqual(ret, lastOverview) {
					printHealthOverview(&ret)
				}
				lastOverview = ret
				if !watch || exit && lastOverview.IsHealthy {
					break
				}
				time.Sleep(2 * time.Second)
			}
		},
	}
	p.InstallAdminFlags(cmd)

	cmd.Flags().BoolVarP(&watch, "watch", "w", false, "Blocks and writes out all cluster health changes")
	cmd.Flags().BoolVarP(&exit, "exit-when-healthy", "e", false, "When used with watch, exits after cluster is back in healthy state")
	return cmd
}

func printHealthOverview(hov *admin.ClusterHealthOverview) {
	out.Section("CLUSTER HEALTH OVERVIEW")
	overviewFormat := `Healthy:               %v
Controller ID:               %v
All nodes:                   %v
Nodes down:                  %v
Leaderless partitions:       %v
Under-replicated partitions: %v
`
	fmt.Printf(overviewFormat, hov.IsHealthy, hov.ControllerID, hov.AllNodes, hov.NodesDown, hov.LeaderlessPartitions, hov.UnderReplicatedPartitions)
}
