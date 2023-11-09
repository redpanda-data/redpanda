// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package partitions

import (
	"fmt"
	"strings"
	"sync"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/types"
	"golang.org/x/sync/errgroup"
)

func newListCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var (
		all          bool
		disabledOnly bool
		partitions   []int
	)
	cmd := &cobra.Command{
		Use:     "list [TOPICS]",
		Aliases: []string{"ls", "describe"},
		Short:   "List partitions in the cluster",
		Long: `List partitions in the cluster

This commands lists the cluster-level metadata of all partitions in the cluster.
It shows the current replica assignments on both brokers and CPU cores for given
topics. By default, it assumes the "kafka" namespace, but you can specify an
internal namespace using the "{namespace}/" prefix.

The REPLICA-CORE column displayed in the output table contains a list of
replicas assignments in the form of: <Node-ID>-<Core>.

EXAMPLES

List all partitions in the cluster.
  rpk cluster partitions list --all

List all partitions in the cluster, filtering for topic foo and bar.
  rpk cluster partitions list foo bar

List only the disabled partitions.
  rpk cluster partitions list -a --only-disabled
`,
		Run: func(cmd *cobra.Command, topics []string) {
			if len(topics) == 0 && !all {
				cmd.Help()
				return
			}
			if len(topics) > 0 && all {
				out.Die("flag '--all' cannot be used with topic filters.\n%v", cmd.UsageString())
			}
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "unable to load config: %v", err)
			out.CheckExitCloudAdmin(p)

			cl, err := adminapi.NewClient(fs, p)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			var clusterPartitions []adminapi.ClusterPartition
			var mu sync.Mutex
			if len(topics) == 0 && all {
				clusterPartitions, err = cl.AllClusterPartitions(cmd.Context(), true, disabledOnly)
				out.MaybeDie(err, "unable to query all partitions in the cluster: %v", err)
			} else {
				g, egCtx := errgroup.WithContext(cmd.Context())
				for _, topic := range topics {
					if topic == "" {
						out.Die("invalid empty topic\n%v", cmd.UsageString())
					}
					nsTopic := strings.SplitN(topic, "/", 2)
					var ns, topicName string
					if len(nsTopic) == 1 {
						ns = "kafka"
						topicName = nsTopic[0]
					} else {
						ns = nsTopic[0]
						topicName = nsTopic[1]
					}
					g.Go(func() error {
						cPartition, err := cl.TopicClusterPartitions(egCtx, ns, topicName, disabledOnly)
						if err != nil {
							return fmt.Errorf("unable to query cluster partition metadata of topic %q: %v", topicName, err)
						}
						mu.Lock()
						clusterPartitions = append(clusterPartitions, cPartition...)
						mu.Unlock()
						return nil
					})
				}
				err := g.Wait()
				out.MaybeDieErr(err)
			}
			if partitions != nil {
				clusterPartitions = filterPartition(clusterPartitions, partitions)
			}
			types.Sort(clusterPartitions)
			tw := out.NewTable("NAMESPACE", "TOPIC", "PARTITION", "LEADER-ID", "REPLICA-CORE", "DISABLED")
			defer tw.Flush()
			for _, p := range clusterPartitions {
				var leader string
				var replicas []string
				if p.LeaderID == nil {
					leader = "-"
				}
				for _, r := range p.Replicas {
					replicas = append(replicas, fmt.Sprintf("%v-%v", r.NodeID, r.Core))
				}
				tw.PrintStructFields(struct {
					Namespace string
					Topic     string
					Partition int
					LeaderID  string
					Replicas  []string
					Disabled  bool
				}{p.Ns, p.Topic, p.PartitionID, leader, replicas, p.Disabled})
			}
		},
	}
	cmd.Flags().BoolVarP(&all, "all", "a", false, "If true, list all partitions in the cluster")
	cmd.Flags().BoolVar(&disabledOnly, "only-disabled", false, "If true, list disabled partitions only")
	cmd.Flags().IntSliceVarP(&partitions, "partition", "p", nil, "Partitions to show current assignments (repeatable)")

	return cmd
}

// filterPartition filters cPartitions and returns a slice with only the
// clusterPartitions with ID present in the partitions slice.
func filterPartition(cPartitions []adminapi.ClusterPartition, partitions []int) (ret []adminapi.ClusterPartition) {
	pm := make(map[int]bool, 0)
	for _, p := range partitions {
		pm[p] = true
	}
	for _, c := range cPartitions {
		if pm[c.PartitionID] {
			ret = append(ret, c)
		}
	}
	return
}
