// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package api

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/cli/ui"
)

type node struct {
	// map[topic-name][]partitions
	leaderParts	map[string][]int
	replicaParts	map[string][]int
}

func NewStatusCommand(
	admin func() (sarama.ClusterAdmin, error),
) *cobra.Command {
	cmd := &cobra.Command{
		Use:	"status",
		Short:	"Get the cluster's status",
		RunE: func(cmd *cobra.Command, args []string) error {
			adm, err := admin()
			if err != nil {
				log.Error("Couldn't initialize API admin")
				return err
			}
			defer adm.Close()
			return executeStatus(adm)
		},
	}
	return cmd
}

func executeStatus(admin sarama.ClusterAdmin) error {
	brokers, _, err := admin.DescribeCluster()

	topics, err := topicsDetail(admin)
	if err != nil {
		return fmt.Errorf(
			"Error fetching the Redpanda topic details: %v",
			err,
		)
	}

	t := ui.NewRpkTable(log.StandardLogger().Out)
	t.SetColWidth(80)
	t.SetAutoWrapText(true)

	t.AppendBulk(partitionInfoRows(brokers, topics))

	t.Render()

	return nil
}

func partitionInfoRows(
	brokers []*sarama.Broker, topics []*sarama.TopicMetadata,
) [][]string {
	rows := [][]string{}
	spacingRow := []string{"", ""}

	nodePartitions := partitionsPerNode(topics)

	idToBroker := map[int]sarama.Broker{}
	nodeIDs := []int{}
	for _, broker := range brokers {
		if broker != nil {
			nodeIDs = append(nodeIDs, int(broker.ID()))
			idToBroker[int(broker.ID())] = *broker
		}
	}

	sort.Ints(nodeIDs)

	rows = append(
		rows,
		[]string{"Redpanda Cluster Status", ""},
		spacingRow,
	)
	for _, nodeID := range nodeIDs {
		node := nodePartitions[nodeID]
		broker := idToBroker[nodeID]
		nodeInfo := fmt.Sprintf("%d (%s)", nodeID, broker.Addr())
		if node == nil {
			rows = append(
				rows,
				[]string{nodeInfo, "(No partitions)"},
				spacingRow,
			)
			continue
		}
		if nodeID < 0 {
			// A negative node ID means the partitions haven't
			// been assigned a leader
			leaderlessRow := []string{
				"(Leaderless)",
				formatTopicsAndPartitions(node.leaderParts),
			}
			rows = append(
				rows,
				leaderlessRow,
				spacingRow,
			)
			continue
		}

		leaderParts := formatTopicsAndPartitions(node.leaderParts)
		leaderRow := []string{
			nodeInfo,
			"Leader: " + leaderParts,
		}
		replicaParts := formatTopicsAndPartitions(node.replicaParts)
		replicaRow := []string{
			"",
			"Replica: " + replicaParts,
		}
		rows = append(
			rows,
			leaderRow,
			spacingRow,
			replicaRow,
			spacingRow,
		)
	}
	return rows
}

func partitionsPerNode(topics []*sarama.TopicMetadata) map[int]*node {
	nodePartitions := map[int]*node{}
	getNode := func(ID int) *node {
		n := nodePartitions[ID]
		if n == nil {
			n = &node{
				leaderParts:	map[string][]int{},
				replicaParts:	map[string][]int{}}
			nodePartitions[ID] = n
		}
		return n
	}
	for _, topic := range topics {
		for _, p := range topic.Partitions {
			leaderID := int(p.Leader)
			leader := getNode(leaderID)
			leader.leaderParts[topic.Name] = append(
				leader.leaderParts[topic.Name],
				int(p.ID))

			for _, r := range p.Replicas {
				replicaID := int(r)
				// Don't list leaders as replicas of their partitions
				if replicaID == leaderID {
					continue
				}

				replica := getNode(replicaID)
				replica.replicaParts[topic.Name] = append(
					replica.replicaParts[topic.Name],
					int(p.ID))
			}
		}
	}
	return nodePartitions
}

func topicsDetail(admin sarama.ClusterAdmin) ([]*sarama.TopicMetadata, error) {
	topics, err := admin.ListTopics()
	if err != nil {
		return nil, err
	}
	topicNames := []string{}
	for name := range topics {
		topicNames = append(topicNames, name)
	}
	return admin.DescribeTopics(topicNames)
}

func formatTopicsAndPartitions(tps map[string][]int) string {
	topicNames := []string{}
	for topicName := range tps {
		topicNames = append(topicNames, topicName)
	}
	sort.Strings(topicNames)
	buf := []string{}
	for _, topicName := range topicNames {
		parts := tps[topicName]
		buf = append(buf, formatTopicPartitions(topicName, parts))
	}
	return strings.Join(buf, "; ")
}

func formatTopicPartitions(name string, partitions []int) string {
	limit := 50
	partitionsNo := len(partitions)
	if partitionsNo <= limit {
		// If the number of partitions is small enough, we can display
		// them all.
		strParts := compress(partitions)
		return fmt.Sprintf("%s: [%s]", name, strings.Join(strParts, ", "))
	}
	// When the # of partitions is too big, the ouput becomes unreadable,
	// so it needs to be truncated.
	return fmt.Sprintf(
		"%s: (%d partitions)",
		name,
		partitionsNo,
	)
}

func compress(is []int) []string {
	length := len(is)
	if length == 0 {
		return []string{}
	}
	sort.Ints(is)
	ranges := []string{}
	for i := 0; i < length; i++ {
		low := is[i]
		high := low
		j := i + 1
		index := j
		for j := i + 1; j < length && is[j] == high+1; j++ {
			high = is[j]
			index = j
		}
		switch {
		case low == high:
			// If there was no range, just add the number.
			ranges = append(ranges, strconv.Itoa(low))
		case high == low+1:
			// If the range is only n - n+1, it makes no sense to
			// add a hyphen.
			ranges = append(
				ranges,
				strconv.Itoa(low),
				strconv.Itoa(high),
			)
			i = index
		default:
			ranges = append(ranges, fmt.Sprintf("%d-%d", low, high))
			i = index
		}
	}
	return ranges
}
