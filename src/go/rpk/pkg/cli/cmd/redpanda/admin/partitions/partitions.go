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
//	"errors"
	"context"
	"strconv"
	"sort"
	"bufio"
	"os"
	"strings"
	"math"
	"math/rand"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/api/admin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/kafka"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/kgo"
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
    cmd :=  &cobra.Command{
        Use:     "drain [BROKER ID]",
        Aliases: []string{"dr"},
        Short:   "Move partitions from a source broker to all other brokers in a cluster based on disk usage.",
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

            // Checking if source broker Id exist
            sourceBrokerCheck := false            
            for _, brokerId := range bs {
            	if brokerId.NodeID == sourceBrokerId && sourceBrokerCheck == false {
            		sourceBrokerCheck = true
            		break
            	}
            }
            if !(sourceBrokerCheck) {
            	out.Die("Source broker is invalid.")
            }

            /*
            1. Get all topic-partition combinations from the source broker. DONE
            2. Sort all topic-partitions by size first in decreasing order. DONE
            3. Get all broker disk usage for all brokers except source broker. DONE
            4. For each topic-partition, get the least used broker, that does not have a replica of this partition and allocate the topic-partition to it. DONE
            5. Store all topic-partition-movement mapping in a data structure. DONE
            6. Print all the movement information and final state of the cluster.
            7. Ask for confirmation and trigger the draining
            */

			// for storing disk usage for all brokers
			//allBrokerDiskUsageMap := getBrokerDiskUsageMap(bs)

			// 1. Get all the partitions on the source broker
			topicPartitionMap := getTopicPartitionMap(m, kgocl, sourceBrokerId)

			// 2. Sort all topic-partitions by size first in decreasing order.
			sort.Slice(topicPartitionMap, func(i, j int) bool {
  				return topicPartitionMap[i].size > topicPartitionMap[j].size
			})

			// 3. Get all broker disk usage for all brokers except source broker.
			diskUsageMap := getBrokerDiskUsageMap(bs)

			// 4. For each topic-partition, get the least used broker, that does not have a replica of this partition and allocate the topic-partition to it.
			//5. Store all topic-partition-movement mapping in a data structure.

			tw := out.NewTable()
			header := func(name string) {				
				tw.Print(name)
				tw.Print(strings.Repeat("=", len(name)))
				tw.Print()
			}

			header("PROPOSED DRAIN PLAN")
			header("PROPOSED DRAIN BROKER ALLOCATION")

			tw.PrintColumn("TOPIC", "PARTITION", "DESTINATION BROKER")
			//tw.PrintColumn("TOPIC", "PARTITION", "DESTINATION BROKER", "DESTINATION BROKER CORE")

			drainMap := make(map[string]int)
			brokerSizeMap := make(map[int]int64)

			nonDrainablePartitionCount := 0
			for _, tp := range topicPartitionMap {
				destinationBroker := getLeastUsedBrokerWithNoReplicas(sourceBrokerId, tp, diskUsageMap)
				if destinationBroker.broker != -1 {
					uS := getUniqueTopicPartitionString(tp.topic, strconv.Itoa(int(tp.partition)))
					drainMap[uS] = destinationBroker.broker
					tw.Print(tp.topic, tp.partition, destinationBroker.broker)
					//tw.Print(tp.topic, tp.partition, destinationBroker.broker, rand.Intn(destinationBroker.cores))
					if val, ok := brokerSizeMap[destinationBroker.broker]; ok {
						brokerSizeMap[destinationBroker.broker] = val - tp.size
					} else {
						brokerSizeMap[destinationBroker.broker] = destinationBroker.free
					}
				} else {
					nonDrainablePartitionCount = nonDrainablePartitionCount + 1
				}
			}

			tw.Print()
			header("PROPOSED DRAIN SUMMARY - BROKERS")
			tw.PrintColumn("BROKER", "CURRENT DISK USAGE %", "PROPOSED DISK USAGE %")
			for k, v := range brokerSizeMap {
				brokerInfo := getBrokerInfo(k, diskUsageMap)
				oldDiskUsage := ((float64(brokerInfo.total) - float64(brokerInfo.free)) / float64(brokerInfo.total))*100
				newDiskUsage := ((float64(brokerInfo.total) - float64(v)) / float64(brokerInfo.total))*100
				tw.Print(k, math.Round(oldDiskUsage*100)/100, math.Round(newDiskUsage*100)/100)
				// If the new disk usage is > 80% of total usage, we terminate the move of partitions
				if newDiskUsage > 80 {
            		out.Die("Propsed Free Disk Space will be less than 20%% of Total Disk Space. Aborting Move.")
            	}
			}

			tw.Print()
			header("PROPOSED DRAIN SUMMARY - PARTITIONS")

			tw.PrintColumn("DRAINABLE PARTITIONS", "NON DRAINABLE PARTITIONS", "TOTAL PARTITIONS")
			tw.Print(len(topicPartitionMap) - nonDrainablePartitionCount, nonDrainablePartitionCount, len(topicPartitionMap))

			//6. Print all the movement information and final state of the cluster.
			tw.Flush()			


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
	
	return cmd
}

/*type NodePartitionCount struct {
	leaderCount int
	replicaCount int
}*/

type TopicPartition struct {
	topic 		string
	partition 	int32
	replicas 	[]int32
	size 		int64
}

type DiskUsage struct {
	broker int
	free int64
	total int64
	cores int
}

func getTopicPartitionMap(m kadm.Metadata, kgocl *kgo.Client, brokerId int) ([]TopicPartition) {
	
	var partitions []TopicPartition
	replicaMap := make(map[string][]int32)

	// START - Fetching partition sizes
   
    // Creating the fetch request to get partition sizes
    var req kmsg.DescribeLogDirsRequest
	for _, t := range m.Topics.Sorted() {
	    for _, pt := range t.Partitions.Sorted() {
            for _, rs := range pt.Replicas {
                if int(rs) == brokerId {
                	var pts []int32
					pts = append(pts, pt.Partition)
					req.Topics = append(req.Topics, kmsg.DescribeLogDirsRequestTopic {
						Topic:      t.Topic,
						Partitions: pts,
					})
					uniqueTPString := getUniqueTopicPartitionString(t.Topic,strconv.Itoa(int(pt.Partition)))
					replicaMap[uniqueTPString] = pt.Replicas
                }
            }
        }
    }

    // Sent the request to fetch partition sizes
	kresps := kgocl.RequestSharded(context.Background(), &req)

	// Creating TopicPartition objects to return
	tpMap := map[string]bool{}

	for _, kresp := range kresps {
		resp := kresp.Resp.(*kmsg.DescribeLogDirsResponse)
		sort.Slice(resp.Dirs, func(i, j int) bool { return resp.Dirs[i].Dir < resp.Dirs[j].Dir })
		for _, dir := range resp.Dirs {
			sort.Slice(dir.Topics, func(i, j int) bool { return dir.Topics[i].Topic < dir.Topics[j].Topic })
			for _, topic := range dir.Topics {
				sort.Slice(topic.Partitions, func(i, j int) bool { return topic.Partitions[i].Partition < topic.Partitions[j].Partition })
				for _, pt := range topic.Partitions {					
					uniqueTPString := getUniqueTopicPartitionString(topic.Topic,strconv.Itoa(int(pt.Partition)))
					_, hasTP := tpMap[uniqueTPString]
					if !hasTP {
						tpMap[uniqueTPString] = true
						partitions = append(partitions, TopicPartition{topic.Topic, pt.Partition, replicaMap[uniqueTPString], pt.Size})
					}
				}
			}
		}		
	}

	// END - Fetched partition sizes
	return partitions
}

func getBrokerDiskUsageMap(bs []admin.Broker) []DiskUsage {
	var diskUsageMap []DiskUsage
        for _, b := range bs {
        	diskUsageMap = append(diskUsageMap, DiskUsage{b.NodeID, b.DiskSpaceItems[0].Free, b.DiskSpaceItems[0].Total, b.NumCores})
        }
	return diskUsageMap
}

func remove(s []int32, r int32) []int32 {
    for i, v := range s {
        if v == r {
            return append(s[:i], s[i+1:]...)
        }
    }
    return s
}

func removeBroker(s []DiskUsage, r int) []DiskUsage {
	for i, v := range s {
        if v.broker == r {
            return append(s[:i], s[i+1:]...)
        }
    }
    return s
}

func contains(s []int32, r int32) bool {
    for _, v := range s {
        if v == r {
            return true
        }
    }    
    return false
}

func getBrokerInfo(bkrId int, brokers []DiskUsage) DiskUsage {
	var bkr DiskUsage
	for _, b := range brokers {
		if b.broker == bkrId {
			bkr = b
		}
	}
	return bkr
}

func getLeastUsedBrokerWithNoReplicas(sourceBroker int, tp TopicPartition, diskUsageMap []DiskUsage) DiskUsage {

	sort.Slice(diskUsageMap, func(i, j int) bool {
		return diskUsageMap[i].free > diskUsageMap[j].free
	})

	otherReplicas := remove(tp.replicas, int32(sourceBroker))
	otherBrokers := removeBroker(diskUsageMap, sourceBroker)

	if len(otherReplicas) == 0 {
		return otherBrokers[rand.Intn(len(otherBrokers))]
	}

	for _, b := range otherBrokers {
		if !contains(otherReplicas, int32(b.broker)) {			
			return b
		}
	}
	
	return DiskUsage{-1, -1, -1, -1}
}

func getUniqueTopicPartitionString(topic string, partition string) string {
	return topic + "-" + partition
}