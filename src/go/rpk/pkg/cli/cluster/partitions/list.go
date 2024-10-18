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
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/redpanda-data/common-go/rpadmin"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/types"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

func newListCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var (
		all             bool
		disabledOnly    bool
		partitions      []int
		nodeIDs         []int
		logFallbackOnce sync.Once
	)
	cmd := &cobra.Command{
		Use:     "list [TOPICS...]",
		Aliases: []string{"ls", "describe"},
		Short:   "List partitions in the cluster",
		Long: `List partitions in the cluster

This commands lists the cluster-level metadata of all partitions in the cluster.
It shows the current replica assignments on both brokers and CPU cores for given
topics. By default, it assumes the "kafka" namespace, but you can specify an
internal namespace using the "{namespace}/" prefix.

The REPLICA-CORE column displayed in the output table contains a list of
replicas assignments in the form of: <Node-ID>-<Core>.

If the DISABLED column contains a '-' value, then it means you are running this
command against a cluster that does not support the underlying API.

ENABLED/DISABLED

Disabling a partition in Redpanda involves prohibiting any data consumption or
production to and from it. All internal processes associated with the partition
are stopped, and it remains unloaded during system startup. This measure aims to
maintain cluster health by preventing issues caused by specific corrupted
partitions that may lead to Redpanda crashes. Although the data remains stored
on disk, Redpanda ceases interaction with the disabled partitions to ensure
system stability.

You may disable/enable partition using 'rpk cluster partitions enable/disable'.	

EXAMPLES

List all partitions in the cluster.
  rpk cluster partitions list --all

List all partitions in the cluster, filtering for topic foo and bar.
  rpk cluster partitions list foo bar

List partitions which replicas are assigned to brokers 1 and 2.
  rpk cluster partitions list foo --node-ids 1,2

List only the disabled partitions.
  rpk cluster partitions list -a --disabled-only

List all in json format.
  rpk cluster partition list -a --format json
`,
		Run: func(cmd *cobra.Command, topics []string) {
			f := p.Formatter
			if h, ok := f.Help([]rpadmin.ClusterPartition{}); ok {
				out.Exit(h)
			}
			if len(topics) == 0 && !all {
				cmd.Help()
				return
			}
			if len(topics) > 0 && all {
				out.Die("flag '--all' cannot be used with topic filters.\n%v", cmd.UsageString())
			}
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			config.CheckExitCloudAdmin(p)

			cl, err := adminapi.NewClient(cmd.Context(), fs, p)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			var clusterPartitions []rpadmin.ClusterPartition
			var mu sync.Mutex
			if len(topics) == 0 && all {
				clusterPartitions, err = cl.AllClusterPartitions(cmd.Context(), true, disabledOnly)
				// If the admin API returns a 404, most likely rpk is talking
				// with an old cluster.
				if he := (*rpadmin.HTTPResponseError)(nil); errors.As(err, &he) {
					if he.Response.StatusCode == http.StatusNotFound {
						out.Die("unable to query all partitions in the cluster: %vYou may need to upgrade the cluster to access this feature or try listing per-topic with: 'rpk cluster partition list [TOPICS...]'", err)
					}
				}
				out.MaybeDie(err, "unable to query all partitions in the cluster: %v", err)
			} else {
				g, egCtx := errgroup.WithContext(cmd.Context())
				for _, topic := range topics {
					if topic == "" {
						out.Die("invalid empty topic\n%v", cmd.UsageString())
					}
					ns, topicName := nsTopic(topic)
					g.Go(func() error {
						cPartition, err := cl.TopicClusterPartitions(egCtx, ns, topicName, disabledOnly)
						if err != nil {
							// If the admin API returns a 404, most likely rpk
							// is talking with an old cluster.
							var he *rpadmin.HTTPResponseError
							isNotFoundErr := errors.As(err, &he) && he.Response.StatusCode == http.StatusNotFound
							if !isNotFoundErr {
								return fmt.Errorf("unable to query cluster partition metadata of topic %q: %v", topicName, err)
							}
							logFallbackOnce.Do(func() {
								if disabledOnly {
									zap.L().Sugar().Warn("Admin API 'GET /v1/cluster/partitions' returned 404, trying now with 'GET /v1/partitions'; --disabled-only flag is not supported in this API version")
								} else {
									zap.L().Sugar().Warn("Admin API 'GET /v1/cluster/partitions' returned 404, trying now with 'GET /v1/partitions'")
								}
							})
							cPartition, err = topicPartitions(egCtx, cl, ns, topicName)
							if err != nil {
								return err
							}
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
			if len(nodeIDs) > 0 {
				clusterPartitions = filterBroker(clusterPartitions, nodeIDs)
			}
			printClusterPartitions(f, clusterPartitions)
		},
	}
	cmd.Flags().BoolVarP(&all, "all", "a", false, "If true, list all partitions in the cluster")
	cmd.Flags().BoolVar(&disabledOnly, "disabled-only", false, "If true, list disabled partitions only")
	cmd.Flags().IntSliceVarP(&partitions, "partition", "p", nil, "List of comma-separated partitions IDs that you wish to filter the results with")
	cmd.Flags().IntSliceVarP(&nodeIDs, "node-ids", "n", nil, "List of comma-separated broker IDs that you wish to filter the results with")

	p.InstallFormatFlag(cmd)
	return cmd
}

func printClusterPartitions(f config.OutFormatter, clusterPartitions []rpadmin.ClusterPartition) {
	types.Sort(clusterPartitions)
	if isText, _, formatted, err := f.Format(clusterPartitions); !isText {
		out.MaybeDie(err, "unable to print partitions in the required format %q: %v", f.Kind, err)
		fmt.Println(formatted)
		return
	}
	tw := out.NewTable("NAMESPACE", "TOPIC", "PARTITION", "LEADER-ID", "REPLICA-CORE", "DISABLED")
	defer tw.Flush()
	for _, p := range clusterPartitions {
		var leader, disabled string
		var replicas []string
		if p.LeaderID == nil {
			leader = "-"
		} else {
			leader = strconv.Itoa(*p.LeaderID)
		}
		if p.Disabled == nil {
			disabled = "-"
		} else {
			disabled = strconv.FormatBool(*p.Disabled)
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
			Disabled  string
		}{p.Ns, p.Topic, p.PartitionID, leader, replicas, disabled})
	}
}

// nsTopic splits a topic string consisting of <namespace>/<topicName> and
// returns each component, if the namespace is not specified, returns 'kafka'.
func nsTopic(nst string) (namespace string, topic string) {
	nsTopic := strings.SplitN(nst, "/", 2)
	if len(nsTopic) == 1 {
		namespace = "kafka"
		topic = nsTopic[0]
	} else {
		namespace = nsTopic[0]
		topic = nsTopic[1]
	}
	return namespace, topic
}

// topicPartitions query the old v1/partitions/ endpoint and parse the result to
// the newer /v1/cluster/partitions format.
func topicPartitions(ctx context.Context, cl *rpadmin.AdminAPI, ns, topicName string) ([]rpadmin.ClusterPartition, error) {
	var ret []rpadmin.ClusterPartition
	tPartitions, err := cl.GetTopic(ctx, ns, topicName)
	if err != nil {
		return nil, fmt.Errorf("unable to query partition metadata of topic %q: %v", topicName, err)
	}
	for _, tp := range tPartitions {
		tp := tp
		ret = append(ret, rpadmin.ClusterPartition{
			Ns:          tp.Namespace,
			Topic:       tp.Topic,
			PartitionID: tp.PartitionID,
			LeaderID:    &tp.LeaderID,
			Replicas:    tp.Replicas,
			Disabled:    nil, // Just to be explicit, old /v1/partitions did not contain any info on disabled.
		})
	}
	return ret, nil
}

// filterPartition filters cPartitions and returns a slice with only the
// clusterPartitions with ID present in the partitions slice.
func filterPartition(cPartitions []rpadmin.ClusterPartition, partitions []int) (ret []rpadmin.ClusterPartition) {
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

// filterBroker filters cPartition and returns a slice with only the
// clusterPartitions with the same brokers present in the Replicas slice.
func filterBroker(cPartitions []rpadmin.ClusterPartition, nodeIDs []int) (ret []rpadmin.ClusterPartition) {
	for _, p := range cPartitions {
		rob := replicaOnBroker(p.Replicas, nodeIDs)
		if rob {
			ret = append(ret, p)
		}
	}
	return
}

// replicaOnBroker returns true when all nodes in the brokers slice
// exist in the Replicas slice. Otherwise, this function returns false.
func replicaOnBroker(replicas rpadmin.Replicas, nodeIDs []int) bool {
	foundCount := 0
	for _, r := range replicas {
		for _, b := range nodeIDs {
			if r.NodeID == b {
				foundCount++
			}
		}
	}
	return foundCount == len(nodeIDs)
}
