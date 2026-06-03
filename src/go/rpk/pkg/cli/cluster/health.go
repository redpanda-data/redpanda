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
	"os"
	"reflect"
	"time"

	"github.com/redpanda-data/common-go/rpadmin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/types"
	"go.uber.org/zap"
)

type healthResponse struct {
	ClusterUUID               *string  `json:"cluster_uuid,omitempty" yaml:"cluster_uuid,omitempty"`
	IsHealthy                 bool     `json:"is_healthy" yaml:"is_healthy"`
	UnhealthyReasons          []string `json:"unhealthy_reasons" yaml:"unhealthy_reasons"`
	ControllerID              int      `json:"controller_id" yaml:"controller_id"`
	AllNodes                  []int    `json:"all_nodes" yaml:"all_nodes"`
	NodesDown                 []int    `json:"nodes_down" yaml:"nodes_down"`
	NodesInRecoveryMode       []int    `json:"nodes_in_recovery_mode" yaml:"nodes_in_recovery_mode"`
	LeaderlessPartitions      []string `json:"leaderless_partitions" yaml:"leaderless_partitions"`
	LeaderlessCount           *int     `json:"leaderless_count,omitempty" yaml:"leaderless_count,omitempty"`
	UnderReplicatedCount      *int     `json:"under_replicated_count,omitempty" yaml:"under_replicated_count,omitempty"`
	UnderReplicatedPartitions []string `json:"under_replicated_partitions" yaml:"under_replicated_partitions"`
	HighDiskUsageNodes        []int    `json:"high_disk_usage_nodes" yaml:"high_disk_usage_nodes"`
}

func newHealthOverviewCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var watch, exit bool
	cmd := &cobra.Command{
		Use:   "health",
		Short: "Queries cluster for health overview",
		Long: `Queries cluster for health overview.

Health overview is created based on the health reports collected periodically
from all nodes in the cluster. A cluster is considered healthy when the
following conditions are met:

  * All cluster nodes are responding
  * All partitions have leaders
  * The cluster controller is present

If the cluster is reported as unhealthy, rpk will exit with code 10.
`,
		Example: `
Basic usage, get cluster health information:
  rpk cluster health

Get cluster health information and watch for changes:
  rpk cluster health --watch

Get cluster health information and exit when the cluster is healthy:
  rpk cluster health --exit-when-healthy
`,
		Args: cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, _ []string) {
			f := p.Formatter
			if f.Kind != "text" && f.Kind != "help" && (watch || exit) {
				out.Die("format type %q cannot be used along with --watch or --exit-when-healthy", f.Kind)
			}
			if h, ok := f.Help(healthResponse{}); ok {
				out.Exit(h)
			}
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			config.CheckExitCloudAdmin(p)

			cl, err := adminapi.NewClient(cmd.Context(), fs, p)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			resp, err := cl.ClusterUUID(cmd.Context())
			var clusterUUID *string
			if err != nil {
				zap.L().Sugar().Warnf("unable to get cluster UUID for the cluster health report: %v; skipping collection and checking cluster health", err)
			} else {
				clusterUUID = &resp.UUID
			}
			// --exit-when-healthy only makes sense with --watch, so we enable
			// watch if --exit-when-healthy is provided.
			watch = exit || watch
			var lastOverview rpadmin.ClusterHealthOverview
			var exit10 bool
			for {
				ret, err := cl.GetHealthOverview(cmd.Context())
				out.MaybeDie(err, "unable to request cluster health: %v", err)
				exit10 = !ret.IsHealthy
				if !reflect.DeepEqual(ret, lastOverview) {
					hr := buildHealthResponses(&ret, clusterUUID)
					if isText, _, s, err := f.Format(hr); !isText {
						out.MaybeDie(err, "unable to print in the required format %q: %v", f.Kind, err)
						fmt.Println(s)
					} else {
						printHealthOverview(hr)
					}
				}
				lastOverview = ret
				if !watch || exit && lastOverview.IsHealthy {
					break
				}
				time.Sleep(2 * time.Second)
			}
			if exit10 {
				// We choose 10 to differentiate from any other error (1), or
				// unhandled panics (2).
				os.Exit(10)
			}
		},
	}
	p.InstallAdminFlags(cmd)
	p.InstallSASLFlags(cmd)
	p.InstallFormatFlag(cmd)

	cmd.Flags().BoolVarP(&watch, "watch", "w", false, "Blocks and writes out all cluster health changes. Only available for --format=text")
	cmd.Flags().BoolVarP(&exit, "exit-when-healthy", "e", false, "Exits when the cluster is back in a healthy state. Only available for --format=text")
	return cmd
}

func buildHealthResponses(hov *rpadmin.ClusterHealthOverview, clusterUUID *string) healthResponse {
	// This is needed as NodesInRecoveryMode can be nil, and the json formatter
	// will print "null" instead of an empty array.
	nodesInRecoveryMode := hov.NodesInRecoveryMode
	if len(nodesInRecoveryMode) == 0 {
		nodesInRecoveryMode = []int{}
	}
	return healthResponse{
		ClusterUUID:               clusterUUID,
		IsHealthy:                 hov.IsHealthy,
		UnhealthyReasons:          hov.UnhealthyReasons,
		ControllerID:              hov.ControllerID,
		AllNodes:                  hov.AllNodes,
		NodesDown:                 hov.NodesDown,
		NodesInRecoveryMode:       nodesInRecoveryMode,
		LeaderlessPartitions:      hov.LeaderlessPartitions,
		LeaderlessCount:           hov.LeaderlessCount,
		UnderReplicatedCount:      hov.UnderReplicatedCount,
		UnderReplicatedPartitions: hov.UnderReplicatedPartitions,
		HighDiskUsageNodes:        hov.HighDiskUsageNodes,
	}
}

func printHealthOverview(hr healthResponse) {
	types.Sort(hr)
	out.Section("CLUSTER HEALTH OVERVIEW")

	// leaderless partitions and under-replicated counts are available starting
	// v23.3.
	lp := "Leaderless partitions:"
	urp := "Under-replicated partitions:"
	if hr.LeaderlessCount != nil {
		lp = fmt.Sprintf("Leaderless partitions (%v):", *hr.LeaderlessCount)
		if *hr.LeaderlessCount > len(hr.LeaderlessPartitions) {
			hr.LeaderlessPartitions = append(hr.LeaderlessPartitions, "...truncated")
		}
	}
	if hr.UnderReplicatedCount != nil {
		urp = fmt.Sprintf("Under-replicated partitions (%v):", *hr.UnderReplicatedCount)
		if *hr.UnderReplicatedCount > len(hr.UnderReplicatedPartitions) {
			hr.UnderReplicatedPartitions = append(hr.UnderReplicatedPartitions, "...truncated")
		}
	}

	tw := out.NewTable()
	defer tw.Flush()
	tw.Print("Healthy:", hr.IsHealthy)
	tw.Print("Unhealthy reasons:", hr.UnhealthyReasons)
	tw.Print("Controller ID:", hr.ControllerID)
	tw.Print("All nodes:", hr.AllNodes)
	tw.Print("Nodes down:", hr.NodesDown)
	if hr.NodesInRecoveryMode != nil {
		tw.Print("Nodes in recovery mode:", hr.NodesInRecoveryMode)
	}
	tw.Print("Nodes with high disk usage:", hr.HighDiskUsageNodes)
	tw.Print(lp, hr.LeaderlessPartitions)
	tw.Print(urp, hr.UnderReplicatedPartitions)
	if hr.ClusterUUID != nil {
		tw.Print("Cluster UUID:", *hr.ClusterUUID)
	}
}
