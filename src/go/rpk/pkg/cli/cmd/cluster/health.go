// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// Package brokers contains commands to talk to the Redpanda's admin brokers
// endpoints.
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

func NewHealthOverviewCommand(fs afero.Fs) *cobra.Command {
	var (
		wait bool
		exit bool
	)
	cmd := cobra.Command{
		Use:   "health",
		Short: "Queries cluster for health overview.",
		Long: `
Queries cluster health overview. Health overview is created based on the health 
reports collected periodically from all nodes in the cluster.

Cluster is considered as healthy when follwing conditions are met:

- all cluster nodes are responding
- all partitions have leaders
- cluster controller is present
`,
		Args: cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, _ []string) {
			p := config.ParamsFromCommand(cmd)
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			cl, err := admin.NewClient(fs, cfg)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			var lastOverview admin.ClusterHealthOverview
			for {
				ret, err := cl.GetHealthOverview()
				out.MaybeDie(err, "unable to request cluster health: %v", err)
				if !reflect.DeepEqual(ret, lastOverview) {
					printHealthOverview(&ret)
				}
				lastOverview = ret
				if !wait || exit && lastOverview.IsHealthy {
					break
				}
				time.Sleep(2 * time.Second)
			}
		},
	}
	cmd.Flags().BoolVarP(&wait, "watch", "w", false, "blocks and writes out all cluster health changes")
	cmd.Flags().BoolVarP(&exit, "exit-when-healthy", "e", false, "when used with wait, exits after cluster is back in healthy state")
	return &cmd
}

func printHealthOverview(hov *admin.ClusterHealthOverview) {
	out.Section("CLUSTER HEALTH OVERVIEW")
	overviewFormat := `Healthy:               %v
Controller ID:         %v
Nodes down:            %v
Leaderless partitions: %v
`
	fmt.Printf(overviewFormat, hov.IsHealthy, hov.ControllerID, hov.NodesDown, hov.LeaderlessPartitions)
}
