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

	"github.com/redpanda-data/common-go/rpadmin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/types"
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
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			config.CheckExitCloudAdmin(p)

			cl, err := adminapi.NewClient(cmd.Context(), fs, p)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			// --exit-when-healthy only makes sense with --watch, so we enable
			// watch if --exit-when-healthy is provided.
			watch = exit || watch
			var lastOverview rpadmin.ClusterHealthOverview
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
	p.InstallSASLFlags(cmd)

	cmd.Flags().BoolVarP(&watch, "watch", "w", false, "Blocks and writes out all cluster health changes")
	cmd.Flags().BoolVarP(&exit, "exit-when-healthy", "e", false, "Exits when the cluster is back in a healthy state")
	return cmd
}

func printHealthOverview(hov *rpadmin.ClusterHealthOverview) {
	types.Sort(hov)
	out.Section("CLUSTER HEALTH OVERVIEW")

	// leaderless partitions and under-replicated counts are available starting
	// v23.3.
	lp := "Leaderless partitions:"
	urp := "Under-replicated partitions:"
	if hov.LeaderlessCount != nil {
		lp = fmt.Sprintf("Leaderless partitions (%v):", *hov.LeaderlessCount)
		if *hov.LeaderlessCount > len(hov.LeaderlessPartitions) {
			hov.LeaderlessPartitions = append(hov.LeaderlessPartitions, "...truncated")
		}
	}
	if hov.UnderReplicatedCount != nil {
		urp = fmt.Sprintf("Under-replicated partitions (%v):", *hov.UnderReplicatedCount)
		if *hov.UnderReplicatedCount > len(hov.UnderReplicatedPartitions) {
			hov.UnderReplicatedPartitions = append(hov.UnderReplicatedPartitions, "...truncated")
		}
	}

	tw := out.NewTable()
	defer tw.Flush()
	tw.Print("Healthy:", hov.IsHealthy)
	tw.Print("Unhealthy reasons:", hov.UnhealthyReasons)
	tw.Print("Controller ID:", hov.ControllerID)
	tw.Print("All nodes:", hov.AllNodes)
	tw.Print("Nodes down:", hov.NodesDown)
	if hov.NodesInRecoveryMode != nil {
		tw.Print("Nodes in recovery mode:", hov.NodesInRecoveryMode)
	}
	tw.Print(lp, hov.LeaderlessPartitions)
	tw.Print(urp, hov.UnderReplicatedPartitions)
}
