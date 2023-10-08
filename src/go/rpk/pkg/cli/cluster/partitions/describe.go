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

func newDescribePartitionAssignmentsCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	type result struct {
		Topic     string
		Partition int
		Leader    int
		NodeCore  []string
	}
	var partitions []int
	cmd := &cobra.Command{
		Use:   "describe",
		Short: "Describe partition replica assignments with CPU cores",
		Long:  helpShowAssignments,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, topics []string) {
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "unable to load config: %v", err)
			out.CheckExitCloudAdmin(p)

			cl, err := adminapi.NewClient(fs, p)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			var (
				ns, t         string
				reqPartitions []int
				results       []result
				mu            sync.Mutex
			)

			g, egCtx := errgroup.WithContext(cmd.Context())
			for _, topic := range topics {
				topic := topic
				g.Go(func() (rerr error) {
					mu.Lock()
					defer mu.Unlock()
					if s := strings.Split(topic, "/"); len(s) == 2 {
						ns = s[0]
						t = s[1]
					} else if len(s) == 1 {
						ns = "kafka"
						t = s[0]
					} else {
						fmt.Printf("invalid format: %s, skipping\n", topic)
						return nil
					}

					if len(partitions) == 0 {
						allPart, err := cl.GetTopic(egCtx, ns, t)
						out.MaybeDie(err, "unable to get topic information: %v\n", err)
						reqPartitions = createPartitionList(len(allPart))
					} else {
						reqPartitions = partitions
					}

					for _, p := range reqPartitions {
						pInfo, err := cl.GetPartition(egCtx, ns, t, p)
						out.MaybeDie(err, "unable to get partition information: %v\n", err)
						results = append(results, result{
							Topic:     ns + "/" + t,
							Partition: p,
							Leader:    pInfo.LeaderID,
							NodeCore:  formatNodeCore(pInfo.Replicas),
						})
					}
					return nil
				})
			}

			if err := g.Wait(); err != nil {
				fmt.Printf("failed to describe partitions: %v\n", err)
			}

			types.Sort(results)

			tw := out.NewTable("TOPIC", "PARTITION", "LEADER", "REPLICA-CORE")
			for _, r := range results {
				tw.Print(r.Topic, r.Partition, r.Leader, r.NodeCore)
			}
			tw.Flush()
		},
	}
	cmd.Flags().IntSliceVarP(&partitions, "partition", "p", nil, "Partitions to show current assignments (repeatable)")
	return cmd
}

func createPartitionList(n int) []int {
	partitions := make([]int, n)
	for i := 0; i <= n-1; i++ {
		partitions[i] = i
	}
	return partitions
}

func formatNodeCore(replicas []adminapi.Replica) []string {
	var result []string
	for _, r := range replicas {
		result = append(result, fmt.Sprintf("%d-%d", r.NodeID, r.Core))
	}
	return result
}

const helpShowAssignments = `Describe partition replica assignments with CPU cores

This command shows current replica assignments on both brokers and CPU cores
for given topics. By default, it assumes the "kafka" namespace, but you can
specify an internal namespace using the "{namespace}/" prefix.

To show replica assignments, use the following syntax:

    rpk cluster partitions describe foo bar

Here, the command shows replica assignments for all partitions in
the topic "foo" and "bar".

You can also specify the partition id with "--partition":

    rpk cluster partitions describe foo --partition 2,3,4
`
