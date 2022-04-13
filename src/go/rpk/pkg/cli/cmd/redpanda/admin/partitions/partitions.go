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
	"fmt"
	"errors"
	"context"
	"strconv"
	"sort"
	"bufio"
	"os"
	"strings"
	"math"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/api/admin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/kafka"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kmsg"
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
		newDrainCommand(fs),
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

func newDrainCommand(fs afero.Fs) *cobra.Command {
	var (
		destinationBrokerId int
	)
        cmd :=  &cobra.Command{
                Use:     "move-from [SOURCE BROKER ID] --destination-broker-id [DESTINATION BROKER ID]",
                Aliases: []string{"mv-frm"},
                Short:   "Move partitions from a source broker to a destination broker in a cluster.",
                Args:    cobra.ExactArgs(1),
                Run: func(cmd *cobra.Command, args []string) {

			p := config.ParamsFromCommand(cmd)
			cfg, err := p.Load(fs)
	        out.MaybeDie(err, "unable to load config: %v", err)

			cl, err := admin.NewClient(fs, cfg)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

            adm, err := kafka.NewAdmin(fs, p, cfg)
            out.MaybeDie(err, "unable to initialize kafka client: %v", err)

            kgocl, err := kafka.NewFranzClient(fs, p, cfg)
            out.MaybeDie(err, "unable to initialize franz-go client: %v", err)

			var m kadm.Metadata
            m, err = adm.Metadata(context.Background())
            out.MaybeDie(err, "unable to request metadata: %v", err)

			sourceBrokerId, err := strconv.Atoi(args[0])
			out.MaybeDie(err, "unable to parse source broker id: %v", err)
            
			bs, err := cl.Brokers()
            out.MaybeDie(err, "unable to request brokers: %v", err)            

            // Checking if source and destination broker Ids are not same
            if destinationBrokerId == sourceBrokerId {
            	out.MaybeDie(errors.New(""),"",errors.New("Destination broker list cannot have source broker id in it."))
            }

            // Checking if source and destination broker Ids exist
            sourceBrokerCheck := false
            destinationBrokerCheck := false
            for _, brokerId := range bs {
            	if brokerId.NodeID == sourceBrokerId && sourceBrokerCheck == false {
            		sourceBrokerCheck = true
            	}
            	if brokerId.NodeID == destinationBrokerId && destinationBrokerCheck == false {
            		destinationBrokerCheck = true
            	}
            }
            if !(sourceBrokerCheck && destinationBrokerCheck) {
            	out.Die("Source or Destination broker is invalid.")
            }

			// for storing disk usage for all brokers
			allBrokerDiskUsageMap := getBrokerDiskUsageMap(bs)			

			// Get all the partitions on the source broker
			leaderPartitions, replicaPartitions := getTopicPartitionMap(m, sourceBrokerId)

			// For each partition, try to fix it on the destination brokers
			for _, t := range leaderPartitions {
				//tw.Print(t.topic, t.partition, remove(t.replicas, int32(brokerId)));
				t.partition = t.partition + 1
				// Verify that no leadership is present on the current broker before proceeding
			}

			// Fetching partition sizes
			var req kmsg.DescribeLogDirsRequest

			for _, t := range replicaPartitions {
				var partitions []int32
				partitions = append(partitions, t.partition)
				req.Topics = append(req.Topics, kmsg.DescribeLogDirsRequestTopic{
					Topic:      t.topic,
					Partitions: partitions,
				})
			}

			kresps := kgocl.RequestSharded(context.Background(), &req)

			tw := out.NewTable()
			header := func(name string) {				
				tw.Print(name)
				tw.Print(strings.Repeat("=", len(name)))
				tw.Print()
			}

			header("DRAIN PLAN")			
			
			var totalPartitionSize int64
			for _, kresp := range kresps {
				resp := kresp.Resp.(*kmsg.DescribeLogDirsResponse)
				sort.Slice(resp.Dirs, func(i, j int) bool { return resp.Dirs[i].Dir < resp.Dirs[j].Dir })
				for _, dir := range resp.Dirs {
					sort.Slice(dir.Topics, func(i, j int) bool { return dir.Topics[i].Topic < dir.Topics[j].Topic })
					for _, topic := range dir.Topics {
						sort.Slice(topic.Partitions, func(i, j int) bool { return topic.Partitions[i].Partition < topic.Partitions[j].Partition })
						for _, partition := range topic.Partitions {							
							totalPartitionSize = totalPartitionSize + partition.Size
						}
					}
				}
				break //TODO: REVISIT WHY DO WE NEED TO BREAK HERE
			}

			oldFDiskUsage := ((float64(allBrokerDiskUsageMap[destinationBrokerId].total) - float64(allBrokerDiskUsageMap[destinationBrokerId].free)) / float64(allBrokerDiskUsageMap[destinationBrokerId].total))*100
			newDiskUsage := ((float64(allBrokerDiskUsageMap[destinationBrokerId].total) - float64(allBrokerDiskUsageMap[destinationBrokerId].free) - float64(totalPartitionSize)) / float64(allBrokerDiskUsageMap[destinationBrokerId].total))*100			

			header("DESTINATION BROKER DETAILS")
			tw.PrintColumn("BROKER ID", "CURRENT DISK USAGE %", "PROPOSED DISK USAGE %", "PARTITIONS TO DRAIN", "DATA TO DRAIN")
			tw.Print(destinationBrokerId, math.Round(oldFDiskUsage*100)/100, math.Round(newDiskUsage*100)/100, len(req.Topics), totalPartitionSize)
			// If the new free disk is < 10% of total usage, we terminate the move of partitions
			if newDiskUsage < 10 {
            	out.Die("New Free Disk Space will be less than 10% of Total Disk Space. Aborting Move.")
            }            
            
            tw.Flush()

            fmt.Println()
            reader := bufio.NewReader(os.Stdin)            
			fmt.Print("Are you sure you want to proceed with the drain?(y/n): ")
			inp, _ := reader.ReadString('\n')
			input := strings.TrimRight(inp, "\n")

			if !(input == "y" || input == "n") {
				out.Die("Invalid input: %v", inp)
			} else if input == "n" {
				out.Die("Aborting the drain.")		
			}

			fmt.Println("Continue with the drain.")
/*
			for _, t := range replicaPartitions {
				pa, err := cl.UpdateReplicas("kafka", t.topic, int(t.partition), sourceBrokerId, destinationBrokerId)
				if err != nil {
					out.MaybeDie(err, "Not able to drain partition: %v", pa.Topic, err)
				} else {
					fmt.Println("Partition is drained out. ", t.topic, " - ", t.partition)
				}				
				break
			}
*/
		},
	}

	cmd.Flags().IntVar(&destinationBrokerId, "destination-broker-id", -1, "Destination Broker Id")
	return cmd
}

type NodePartitionCount struct {
	leaderCount int
	replicaCount int
}

type TopicPartition struct {
	topic 		string
	partition 	int32
	replicas 	[]int32
	size	 	int64
}

type DiskUsage struct {
	free int64
	total int64
}

func getTopicPartitionMap(m kadm.Metadata, brokerId int) ([]TopicPartition, []TopicPartition) {

	var leaderPartitions []TopicPartition
	var replicaPartitions []TopicPartition

	for _, t := range m.Topics.Sorted() {
	        for _, pt := range t.Partitions.Sorted() {
                	for _, rs := range pt.Replicas {
                        	if int(rs) == brokerId {
                                        if int(pt.Leader) == brokerId {
                                        	leaderPartitions = append(leaderPartitions, TopicPartition{t.Topic, pt.Partition, pt.Replicas, 0})
                                        } else {
                                        	replicaPartitions = append(replicaPartitions, TopicPartition{t.Topic, pt.Partition, pt.Replicas, 0})
					}
                                }
                        }
                }
        }

	return leaderPartitions,replicaPartitions
}

func getBrokerDiskUsageMap(bs []admin.Broker) (map[int]DiskUsage) {

	dBrokerDiskUsage := make(map[int]DiskUsage)

        for _, b := range bs {
        	dBrokerDiskUsage[b.NodeID] = DiskUsage{b.DiskSpaceItems[0].Free, b.DiskSpaceItems[0].Total}
        }
	return dBrokerDiskUsage

}

func remove(s []int32, r int32) []int32 {
    for i, v := range s {
        if v == r {
            return append(s[:i], s[i+1:]...)
        }
    }
    return s
}

func getLeastUsedBroker(dBrokerDiskUsageMap map[int]DiskUsage) {
	broker := dBrokerDiskUsageMap[0]
	for i := 1; i < len(dBrokerDiskUsageMap); i++ {
		if dBrokerDiskUsageMap[i].free > broker.free {
			broker = dBrokerDiskUsageMap[i]
		}
	}
	fmt.Println(broker)
}