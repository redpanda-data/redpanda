// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// Package partitions contains commands to talk to the Redpanda's admin partitions
// endpoints.
package partitions

import (
	"context"
	"strconv"
	"strings"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/kafka"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
)

// NewCommand returns the partitions admin command.
func NewCommand(fs afero.Fs) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "partitions",
		Short: "View and configure Redpanda partitions through the admin listener.",
		Args:  cobra.ExactArgs(0),
	}
	cmd.AddCommand(
		newListCommand(fs),
		newMoveAllCommand(fs),
	)
	return cmd
}

func newListCommand(fs afero.Fs) *cobra.Command {
	var leaderOnly bool
	cmd := &cobra.Command{
		Use:     "list [BROKER ID]",
		Aliases: []string{"ls"},
		Short:   "List the partitions in a broker in the cluster.",
		Args:    cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			brokerID, err := strconv.Atoi(args[0])
			out.MaybeDie(err, "invalid broker %s: %v", args[0], err)
			if brokerID < 0 {
				out.Die("invalid negative broker id %v", brokerID)
			}

			p := config.ParamsFromCommand(cmd)
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			adm, err := kafka.NewAdmin(fs, p, cfg)
			out.MaybeDie(err, "unable to initialize kafka client: %v", err)
			defer adm.Close()

			var m kadm.Metadata
			m, err = adm.Metadata(context.Background())
			out.MaybeDie(err, "unable to request metadata: %v", err)

			tw := out.NewTable("TOPIC", "PARTITION", "IS-LEADER")
			defer tw.Flush()

			for _, t := range m.Topics.Sorted() {
				for _, pt := range t.Partitions.Sorted() {
					for _, rs := range pt.Replicas {
						if int(rs) == brokerID {
							var isLeader bool
							if int(pt.Leader) == brokerID {
								isLeader = true
								tw.Print(t.Topic, pt.Partition, isLeader)
							}
							if !leaderOnly && !isLeader {
								tw.Print(t.Topic, pt.Partition, isLeader)
							}
						}
					}
				}
			}
		},
	}

	cmd.Flags().BoolVarP(&leaderOnly, "leader-only", "l", false, "print the partitions on broker which are leaders")

	return cmd
}

func newMoveAllCommand(fs afero.Fs) *cobra.Command {
	var (
		sourceBrokerId string
		destinationBrokerIds string
	)
        cmd :=  &cobra.Command{
                Use:     "move-all --source-broker-id [SOURCE BROKER ID] --destination-broker-ids [DESTINATION BROKER IDs]",
                Aliases: []string{"mv-all"},
                Short:   "Move all the partitions from a source broker to multiple destination brokers in a cluster.",
                Args:    cobra.ExactArgs(0),
                Run: func(cmd *cobra.Command, args []string) {

			p := config.ParamsFromCommand(cmd)
                        cfg, err := p.Load(fs)
                        out.MaybeDie(err, "unable to load config: %v", err)

                        adm, err := kafka.NewAdmin(fs, p, cfg)
                        out.MaybeDie(err, "unable to initialize kafka client: %v", err)

			var m kadm.Metadata
                        m, err = adm.Metadata(context.Background())
                        out.MaybeDie(err, "unable to request metadata: %v", err)

                        // Check if Destination Broker list does not have Source Broker ID
                        destinationBrokersMap := make(map[int]NodePartitionCount)
                        for _, db := range strings.Split(destinationBrokerIds, ",") {
                        	dbId, err := strconv.Atoi(db)
                        	out.MaybeDie(err, "unable to parse destination broker id: %v", err)
                        	leaderPartitions, replicaPartitions := getTopicPartitionMap(m, dbId)
                        	destinationBrokersMap[dbId] = NodePartitionCount{len(leaderPartitions), len(replicaPartitions)}
                        }

                        tw := out.NewTable("DESTINATION", "LEADER_PARTITION_COUNT", "REPLICA_PARTITION_COUNT")
                        defer tw.Flush()

                        /*
                        For debugging leader and replica count
                        for dbId, db := range destinationBrokersMap {
                        	tw.Print(dbId, db.leaderCount, db.replicaCount)	
                        }
                        */
                        
			brokerId, err := strconv.Atoi(sourceBrokerId)
			leaderPartitions, replicaPartitions := getTopicPartitionMap(m, brokerId)

                        for _, t := range leaderPartitions {				
				// Transfer leadership to the replica on a broker, which has lower number of leaders
				tw.Print(t.topic, t.partition, remove(t.replicas, int32(brokerId)));				
				// Verify that no leadership is present on the current broker before proceeding
			}

			for _, t := range replicaPartitions {
                                tw.Print(t.topic, t.partition, "NO")
                                break
                        }
		},
	}

	cmd.Flags().StringVar(&sourceBrokerId, "source-broker-id", "", "Source Broker Id")
	cmd.Flags().StringVar(&destinationBrokerIds, "destination-broker-ids", "", "Destination Broker Ids")

	return cmd
}

type NodePartitionCount struct {
	leaderCount int
	replicaCount int
}

type TopicPartition struct {
	topic string
	partition int32
	replicas []int32
}

func getTopicPartitionMap(m kadm.Metadata, brokerId int) ([]TopicPartition, []TopicPartition) {

	var leaderPartitions []TopicPartition
	var replicaPartitions []TopicPartition

	for _, t := range m.Topics.Sorted() {
	        for _, pt := range t.Partitions.Sorted() {
                	for _, rs := range pt.Replicas {
                        	if int(rs) == brokerId {
                                        if int(pt.Leader) == brokerId {
                                        	leaderPartitions = append(leaderPartitions, TopicPartition{t.Topic, pt.Partition, pt.Replicas})
                                        } else {
                                        	replicaPartitions = append(replicaPartitions, TopicPartition{t.Topic, pt.Partition, pt.Replicas})
					}
                                }
                        }
                }
        }

	return leaderPartitions,replicaPartitions
}

func remove(s []int32, r int32) []int32 {
    for i, v := range s {
        if v == r {
            return append(s[:i], s[i+1:]...)
        }
    }
    return s
}
