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
	"math/rand"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/redpanda-data/common-go/rpadmin"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/types"
	"golang.org/x/sync/errgroup"
)

type newAssignment struct {
	Namespace   string           `json:"ns"`
	Topic       string           `json:"topic"`
	Partition   int              `json:"partition_id"`
	OldReplicas rpadmin.Replicas `json:"old_replicas"`
	NewReplicas rpadmin.Replicas `json:"new_replicas"`
	Error       string           `json:"error,omitempty"`
}

func newMovePartitionReplicasCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var partitionsFlag []string
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

			cl, err := adminapi.NewClient(cmd.Context(), fs, p)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			newAssignmentList, coreAssignmentList, err := parseAssignments(cmd.Context(), cl, partitionsFlag, topics)
			out.MaybeDie(err, "unable to parse new assignments: %v", err)
			if len(newAssignmentList) == 0 && len(coreAssignmentList) == 0 {
				out.Exit("No movements are required.")
			}

			coreAssignmentOn, err := isLocalCoreAssignmentOn(cmd.Context(), cl)
			out.MaybeDie(err, "unable to determine if node_local_core_assignment feature is active: %v", err)
			zap.L().Sugar().Debugf("feature node_local_core_assignment, active: %v", coreAssignmentOn)

			// We fill the missing core assignments (-1) with random cores. For
			// old brokers this is expected, for new brokers (coreAssignmentOn),
			// the core value is arbitrary, so it does not matter.
			newAssignmentList, err = fillAssignmentList(cmd.Context(), cl, newAssignmentList)
			out.MaybeDie(err, "unable to parse assignment list with existing partition replicas: %v", err)
			var (
				wg sync.WaitGroup
				mu sync.Mutex
			)
			for i, newa := range newAssignmentList {
				i, newa := i, newa
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
			if coreAssignmentOn {
				for i, newCore := range coreAssignmentList {
					i, newCore := i, newCore
					for _, rc := range newCore.NewReplicas {
						rc := rc
						wg.Add(1)
						go func(newCore newAssignment, rc rpadmin.Replica) {
							defer wg.Done()
							zap.L().Sugar().Debugf("Partition %v: assigning core %v in node %v ", newCore.Partition, rc.Core, rc.NodeID)
							err := cl.UpdatePartitionReplicaCore(cmd.Context(), newCore.Namespace, newCore.Topic, newCore.Partition, rc.NodeID, rc.Core)
							mu.Lock()
							defer mu.Unlock()
							if he := (*rpadmin.HTTPResponseError)(nil); errors.As(err, &he) {
								body, bodyErr := he.DecodeGenericErrorBody()
								if bodyErr == nil {
									coreAssignmentList[i].Error = body.Message
								} else {
									coreAssignmentList[i].Error = err.Error()
								}
							}
						}(newCore, rc)
					}
				}
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
			if successes > 0 {
				fmt.Println()
				fmt.Printf("Successfully began %d partition movement(s).\n\nCheck the movement status with 'rpk cluster partitions move-status' or see new assignments with 'rpk topic describe -p TOPIC'.\n", successes)
			}
		},
	}
	cmd.Flags().StringArrayVarP(&partitionsFlag, "partition", "p", nil, "Topic-partitions to move and new replica locations (repeatable)")
	cmd.MarkFlagRequired("partition")

	p.InstallFormatFlag(cmd)
	return cmd
}

// parseAssignments parses the arguments and partition flag of the partition
// move command, returning the new node and core assignments.
func parseAssignments(ctx context.Context, cl *rpadmin.AdminAPI, partitionsFlag, topics []string) (nodeAssignmentList, coreAssignmentList []newAssignment, err error) {
	var mu sync.Mutex
	// Concurrently parse the requested partitions and
	// find current replica assignments
	g, egCtx := errgroup.WithContext(ctx)
	for _, pFlag := range partitionsFlag {
		pFlag := pFlag
		g.Go(func() error {
			if len(topics) > 0 { // foo -p 0:1,2,3
				for _, t := range topics {
					_, _, part, err := extractNTP(t, pFlag)
					if err != nil {
						return fmt.Errorf("failed to extract topic/partition: %s", err)
					}
					var ns, topic string
					if nt := strings.Split(t, "/"); len(nt) == 1 {
						ns = "kafka"
						topic = nt[0]
					} else {
						ns = nt[0]
						topic = nt[1]
					}
					current, err := cl.GetPartition(egCtx, ns, topic, part)
					if err != nil {
						return fmt.Errorf("unable to get partition: %s", err)
					}
					newReplicas, coreChanges, err := extractReplicaChanges(pFlag, current.Replicas)
					if err != nil {
						return fmt.Errorf("unable to configure new replicas: %v", err)
					}

					if areReplicasEqual(current.Replicas, newReplicas) {
						continue // noNewAssignment
					}
					mu.Lock()
					nodeAssignmentList = append(nodeAssignmentList, newAssignment{
						Namespace:   ns,
						Topic:       topic,
						Partition:   part,
						OldReplicas: current.Replicas,
						NewReplicas: newReplicas,
					})
					coreAssignmentList = append(coreAssignmentList, newAssignment{
						Namespace:   ns,
						Topic:       topic,
						Partition:   part,
						OldReplicas: current.Replicas,
						NewReplicas: coreChanges,
					})
					mu.Unlock()
				}
			} else { // -p foo/0:1,2,3
				ns, topic, part, err := extractNTP("", pFlag)
				if err != nil {
					return fmt.Errorf("failed to extract topic/partition: %s", err)
				}
				current, err := cl.GetPartition(egCtx, ns, topic, part)
				if err != nil {
					return fmt.Errorf("unable to get partition: %s", err)
				}
				newReplicas, coreChanges, err := extractReplicaChanges(pFlag, current.Replicas)
				if err != nil {
					return fmt.Errorf("unable to configure new replicas: %v", err)
				}
				if areReplicasEqual(current.Replicas, newReplicas) {
					return nil // noNewAssignment
				}
				mu.Lock()
				nodeAssignmentList = append(nodeAssignmentList, newAssignment{
					Namespace:   ns,
					Topic:       topic,
					Partition:   part,
					OldReplicas: current.Replicas,
					NewReplicas: newReplicas,
				})
				coreAssignmentList = append(coreAssignmentList, newAssignment{
					Namespace:   ns,
					Topic:       topic,
					Partition:   part,
					OldReplicas: current.Replicas,
					NewReplicas: coreChanges,
				})
				mu.Unlock()
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, nil, fmt.Errorf("failed to parse the arguments: %v", err)
	}
	types.Sort(nodeAssignmentList)
	types.Sort(coreAssignmentList)
	return nodeAssignmentList, coreAssignmentList, nil
}

// fillAssignmentList checks for the replicas in the assignment list that has
// a core value -1 and fills it with a random core value.
func fillAssignmentList(ctx context.Context, cl *rpadmin.AdminAPI, assignmentList []newAssignment) ([]newAssignment, error) {
	var (
		brokerReqs    = make(map[int]struct{})
		knownNodeCore = make(map[int]int)
		mu            sync.Mutex
	)
	for _, newa := range assignmentList {
		for _, nr := range newa.NewReplicas {
			if nr.Core == -1 {
				brokerReqs[nr.NodeID] = struct{}{}
			}
		}
	}
	g, egCtx := errgroup.WithContext(ctx)
	for node := range brokerReqs {
		node := node
		g.Go(func() error {
			broker, err := cl.Broker(egCtx, node)
			mu.Lock()
			defer mu.Unlock()
			knownNodeCore[node] = broker.NumCores
			return err
		})
	}
	if err := g.Wait(); err != nil {
		return nil, fmt.Errorf("unable to find core counts: %v", err)
	}

	var filledAssignments []newAssignment
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	for _, newa := range assignmentList {
		newa := newa
		for j, nr := range newa.NewReplicas {
			if nr.Core == -1 {
				numCore := knownNodeCore[nr.NodeID]
				newa.NewReplicas[j].Core = rng.Intn(numCore)
			}
		}
		filledAssignments = append(filledAssignments, newa)
	}
	return filledAssignments, nil
}

var (
	ntpRe     *regexp.Regexp
	ntpReOnce sync.Once
)

// extractNTP parses the namespace/topic/partition out of the partition flag
// with format; foo/0:1,2,3 or 0:1,2,3.
func extractNTP(topic string, partitionFlag string) (ns string, t string, p int, err error) {
	ntpReOnce.Do(func() {
		// This regexp captures a sequence of characters before a colon.
		// It can be either a number alone, or a number preceded by any number
		// of '[strings]/' segments.
		ntpRe = regexp.MustCompile(`^((?:[^:]+/)?\d+):.*$`)
	})
	// Match[0]: Full Match.
	// Match[1]: String before the colon ([ns/]t/p).
	m := ntpRe.FindStringSubmatch(partitionFlag)
	if len(m) == 0 {
		return "", "", -1, fmt.Errorf("invalid format for %s; check --help text", partitionFlag)
	}
	beforeColon := m[1]
	// If a topic is provided, we only parse the partition and return.
	if topic != "" {
		if strings.Contains(beforeColon, "/") {
			return "", "", -1, fmt.Errorf("unable to parse --partition %v with topic %q: providing a topic as an argument and in the --partition flag is not allowed", partitionFlag, topic)
		}
		p, err = strconv.Atoi(beforeColon)
		if err != nil {
			return "", "", -1, fmt.Errorf("%s", err)
		}
		return
	}
	// Parse either namespace/topic/partition or topic/partition:
	n := strings.Split(beforeColon, "/")
	switch len(n) {
	case 3:
		ns = n[0]
		t = n[1]
		p, err = strconv.Atoi(n[2])
		if err != nil {
			return "", "", -1, fmt.Errorf("%s", err)
		}
	case 2:
		ns = "kafka"
		t = n[0]
		p, err = strconv.Atoi(n[1])
		if err != nil {
			return "", "", -1, fmt.Errorf("%s", err)
		}
	default:
		return "", "", -1, fmt.Errorf("invalid format for %s; check --help text", partitionFlag)
	}
	return ns, t, p, nil
}

var (
	replicaRe     *regexp.Regexp
	replicaReOnce sync.Once
)

// extractReplicaChanges parses the node-core from the partition flag with format:
// foo/0:1-0,2-1,3-2 or 0:1,2,3. It returns the full rpadmin.Replicas changes,
// and a subset of only the core changes requested.
func extractReplicaChanges(partitionFlag string, currentReplicas rpadmin.Replicas) (allReplicas, coreReplicas rpadmin.Replicas, err error) {
	replicaReOnce.Do(func() {
		// This regexp captures the sequence of numbers (nodes) after a colon.
		// The sequence can be a single number, two numbers separated by a
		// hyphen (node-core), or multiple of such numbers separated by commas.
		replicaRe = regexp.MustCompile(`^[^:]+:(\d+(?:-\d+)?(?:,\d+(?:-\d+)?)*)$`)
	})
	// Match[0]: Full Match.
	// Match[1]: String after the colon. (node[-core],...)
	m := replicaRe.FindStringSubmatch(partitionFlag)
	if len(m) == 0 {
		return nil, nil, fmt.Errorf("invalid format for %s; check --help text", partitionFlag)
	}
	nodeCoresChanges := strings.Split(m[1], ",")
	if len(nodeCoresChanges) != len(currentReplicas) {
		return nil, nil, fmt.Errorf("cannot configure %s; cannot modify replication factor, current replica count: %v; replica count requested: %v. You may use 'rpk topic alter-config [TOPIC] -s replication.factor=X' to modify the replication factor", partitionFlag, len(currentReplicas), len(nodeCoresChanges))
	}
	for _, nodeCore := range nodeCoresChanges {
		if split := strings.Split(nodeCore, "-"); len(split) == 1 { // Node change: -p 0:1,2,3
			node, _ := strconv.Atoi(split[0])
			core := findCore(node, currentReplicas)
			allReplicas = append(allReplicas, rpadmin.Replica{NodeID: node, Core: core})
		} else { // Node/Core changes: -p 0:1-0,2-3,3
			node, _ := strconv.Atoi(split[0])
			currentCore := findCore(node, currentReplicas)
			if currentCore == -1 { // -1 == not found == new node
				return nil, nil, fmt.Errorf("node '%v': this command does not support updating cores for replicas on new nodes", node)
			}
			newCore, _ := strconv.Atoi(split[1])
			allReplicas = append(allReplicas, rpadmin.Replica{NodeID: node, Core: newCore})
			// We only append core changes. If the requested core is the same, we don't bother.
			if currentCore != newCore {
				coreReplicas = append(coreReplicas, rpadmin.Replica{NodeID: node, Core: newCore})
			}
		}
	}
	return
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

func isLocalCoreAssignmentOn(ctx context.Context, cl *rpadmin.AdminAPI) (bool, error) {
	features, err := cl.GetFeatures(ctx)
	if err != nil {
		return false, fmt.Errorf("error trying to get feature lists: %v", err)
	}
	var foundActive bool
	for _, f := range features.Features {
		if f.Name == "node_local_core_assignment" && f.State == "active" {
			foundActive = true
		}
	}
	return foundActive, nil
}

// areReplicasEqual check if both replicas are equal.
func areReplicasEqual(current, requested rpadmin.Replicas) bool {
	if len(current) != len(requested) {
		return false
	}
	types.Sort(current)
	types.Sort(requested)
	return reflect.DeepEqual(current, requested)
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
