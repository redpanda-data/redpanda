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
	"errors"
	"fmt"
	"math/rand"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redpanda-data/common-go/rpadmin"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/types"
	"golang.org/x/sync/errgroup"
)

func newMovePartitionReplicasCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var (
		ns         string
		topic      string
		partitions []string
	)
	type newAssignment struct {
		Namespace   string           `json:"ns"`
		Topic       string           `json:"topic"`
		Partition   int              `json:"partition_id"`
		OldReplicas rpadmin.Replicas `json:"old_replicas"`
		NewReplicas rpadmin.Replicas `json:"new_replicas"`
		Error       string           `json:"error,omitempty"`
	}
	cmd := &cobra.Command{
		Use:   "move",
		Short: "Move partition replicas across nodes / cores",
		Long:  helpAlterAssignments,
		Run: func(cmd *cobra.Command, topics []string) {
			f := p.Formatter
			if h, ok := f.Help([]newAssignment{}); ok {
				out.Exit(h)
			}

			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			config.CheckExitCloudAdmin(p)

			cl, err := adminapi.NewClient(fs, p)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			var (
				mu                sync.Mutex
				newAssignmentList []newAssignment
				wg                sync.WaitGroup
				brokerReqs        = make(map[int]struct{})
				knownNodeCore     = make(map[int]int)
			)

			// Concurrently parse the requested partitions and
			// find current replica assignments
			g, egCtx := errgroup.WithContext(cmd.Context())
			for _, partition := range partitions {
				partition := partition
				g.Go(func() error {
					if len(topics) > 0 { // foo -p 0:1,2,3
						for _, t := range topics {
							_, _, part, err := extractNTP(t, partition)
							out.MaybeDie(err, "failed to extract topic/partition: %s\n", err)

							if nt := strings.Split(t, "/"); len(nt) == 1 {
								ns = "kafka"
								topic = nt[0]
							} else {
								ns = nt[0]
								topic = nt[1]
							}

							current, err := cl.GetPartition(egCtx, ns, topic, part)
							out.MaybeDie(err, "unable to get partition: %s\n", err)

							newReplicas, err := configureReplicas(partition, current.Replicas)
							out.MaybeDie(err, "unable to configure new replicas: %v\n", err)

							mu.Lock()
							newAssignmentList = append(newAssignmentList, newAssignment{
								Namespace:   ns,
								Topic:       topic,
								Partition:   part,
								OldReplicas: current.Replicas,
								NewReplicas: newReplicas,
							})
							mu.Unlock()
						}
					} else { // -p foo/0:1,2,3
						ns, topic, part, err := extractNTP("", partition)
						out.MaybeDie(err, "failed to extract topic/partition: %s\n", err)

						current, err := cl.GetPartition(egCtx, ns, topic, part)
						out.MaybeDie(err, "unable to get partition: %s\n", err)

						newReplicas, err := configureReplicas(partition, current.Replicas)
						out.MaybeDie(err, "unable to configure new replicas: %v\n", err)

						mu.Lock()
						newAssignmentList = append(newAssignmentList, newAssignment{
							Namespace:   ns,
							Topic:       topic,
							Partition:   part,
							OldReplicas: current.Replicas,
							NewReplicas: newReplicas,
						})
						mu.Unlock()
					}
					return nil
				})
			}

			if err := g.Wait(); err != nil {
				out.Die("failed to parse the arguments: %v\n", err)
			}

			for _, newa := range newAssignmentList {
				for _, nr := range newa.NewReplicas {
					if nr.Core == -1 {
						brokerReqs[nr.NodeID] = struct{}{}
					}
				}
			}
			for node := range brokerReqs {
				node := node
				g.Go(func() error {
					broker, err := cl.Broker(cmd.Context(), node)
					mu.Lock()
					defer mu.Unlock()
					knownNodeCore[node] = broker.NumCores
					return err
				})
			}

			if err := g.Wait(); err != nil {
				out.Die("unable to find core counts", err)
			}

			rng := rand.New(rand.NewSource(time.Now().UnixNano()))
			for i, newa := range newAssignmentList {
				i := i
				newa := newa
				for j, nr := range newa.NewReplicas {
					if nr.Core == -1 {
						numCore := knownNodeCore[nr.NodeID]
						newa.NewReplicas[j].Core = rng.Intn(numCore)
					}
				}
				wg.Add(1)
				go func(newa newAssignment) {
					defer wg.Done()
					err := cl.MoveReplicas(cmd.Context(), newa.Namespace, newa.Topic, newa.Partition, newa.NewReplicas)
					mu.Lock()
					defer mu.Unlock()
					if he := (*rpadmin.HTTPResponseError)(nil); errors.As(err, &he) {
						body, bodyErr := he.DecodeGenericErrorBody()
						if bodyErr == nil {
							newAssignmentList[i].Error = body.Message
						} else {
							newAssignmentList[i].Error = err.Error()
						}
					}
				}(newa)
			}
			wg.Wait()

			types.Sort(newAssignmentList)

			if isText, _, formatted, err := f.Format(newAssignmentList); !isText {
				out.MaybeDie(err, "unable to print partitions in the required format %q: %v", f.Kind, err)
				fmt.Println(formatted)
				return
			}

			tw := out.NewTable("NAMESPACE", "TOPIC", "PARTITION", "OLD-REPLICAS", "NEW-REPLICAS", "ERROR")
			var successes int
			for _, newa := range newAssignmentList {
				tw.PrintStructFields(newa)
				if newa.Error == "" {
					successes++
				}
			}
			tw.Flush()
			fmt.Println()
			fmt.Printf("Successfully began %d partition movement(s).\n\nCheck the movement status with 'rpk cluster partitions move-status' or see new assignments with 'rpk topic describe -p TOPIC'.\n", successes)
		},
	}
	cmd.Flags().StringArrayVarP(&partitions, "partition", "p", nil, "Topic-partitions to move and new replica locations (repeatable)")
	cmd.MarkFlagRequired("partition")

	p.InstallFormatFlag(cmd)
	return cmd
}

var (
	ntpRe     *regexp.Regexp
	ntpReOnce sync.Once
)

// extractNTP parses the partition flag with format; foo/0:1,2,3 or 0:1,2,3.
// It extracts letters before the colon and formats it.
func extractNTP(topic string, ntp string) (ns string, t string, p int, err error) {
	ntpReOnce.Do(func() {
		ntpRe = regexp.MustCompile(`^((?:[^:]+/)?\d+):.*$`)
	})
	m := ntpRe.FindStringSubmatch(ntp)
	if len(m) == 0 {
		return "", "", -1, fmt.Errorf("invalid format for %s", ntp)
	}
	beforeColon := m[1]
	if topic != "" {
		p, err = strconv.Atoi(beforeColon)
		if err != nil {
			return "", "", -1, fmt.Errorf("%s", err)
		}
	} else if n := strings.Split(beforeColon, "/"); len(n) == 3 {
		ns = n[0]
		t = n[1]
		p, err = strconv.Atoi(n[2])
		if err != nil {
			return "", "", -1, fmt.Errorf("%s", err)
		}
	} else if len(n) == 2 {
		ns = "kafka"
		t = n[0]
		p, err = strconv.Atoi(n[1])
		if err != nil {
			return "", "", -1, fmt.Errorf("%s", err)
		}
	} else {
		return "", "", -1, fmt.Errorf("invalid format for %s", ntp)
	}
	return ns, t, p, nil
}

var (
	replicaRe     *regexp.Regexp
	replicaReOnce sync.Once
)

// configureReplicas parses the partition flag with format; foo/0:1-0,2-1,3-2 or 0:1,2,3
// It extracts letters after the colon and return as adminapi.Replicas.
func configureReplicas(partition string, currentReplicas rpadmin.Replicas) (rpadmin.Replicas, error) {
	replicaReOnce.Do(func() {
		replicaRe = regexp.MustCompile(`^[^:]+:(\d+(?:-\d+)?(?:,\d+(?:-\d+)?)*)$`)
	})
	m := replicaRe.FindStringSubmatch(partition)
	if len(m) == 0 {
		return nil, fmt.Errorf("invalid format for %s", partition)
	}
	var newReplicas rpadmin.Replicas
	for _, nodeCore := range strings.Split(m[1], ",") {
		if split := strings.Split(nodeCore, "-"); len(split) == 1 {
			node, _ := strconv.Atoi(split[0])
			core := findCore(node, currentReplicas)
			newReplicas = append(newReplicas, rpadmin.Replica{
				NodeID: node,
				Core:   core,
			})
		} else {
			node, _ := strconv.Atoi(split[0])
			core, _ := strconv.Atoi(split[1])
			newReplicas = append(newReplicas, rpadmin.Replica{
				NodeID: node,
				Core:   core,
			})
		}
	}
	return newReplicas, nil
}

// findCore finds a shard (CPU core) where an existing replica is
// assigned on. Returns '-1' for a new node.
func findCore(nodeID int, currentReplicas rpadmin.Replicas) int {
	for _, i := range currentReplicas {
		if nodeID == i.NodeID {
			return i.Core
		}
	}
	return -1
}

const helpAlterAssignments = `Move partition replicas across nodes / cores.

This command changes replica assignments for given partitions. By default, it
assumes the "kafka" namespace, but you can specify an internal namespace using
the "{namespace}/" prefix.

To move replicas, use the following syntax:

    rpk cluster partitions move foo --partition 0:1,2,3 -p 1:2,3,4

Here, the command assigns new replicas for partition 0 to brokers [1, 2, 3] and
for partition 1 to brokers [2, 3, 4] for the topic "foo".

You can also specify the core id with "-{core_id}" where the new replicas
should be placed:

    rpk cluster partitions move foo -p 0:1-0,2-0,3-0

Here all new replicas [1, 2, 3] will be assigned on core 0 on the nodes.

The command does not change a "core" assignment unless it is explicitly
specified. When a core is not specified for a new node, the command randomly
picks a core and assign a replica on the core.

Topic arguments are optional. For more control, you can specify the topic name
in the "--partition" flag:

    rpk cluster partitions move -p foo/0:1,2,3 -p kafka_internal/tx/0:1-0,2-0,3-0
`
