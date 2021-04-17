// Copyright 2021 Vectorized, Inc.
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
	"sort"

	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/cli/ui"
)

func NewOffsetsCommand(
	client func() (sarama.Client, error),
	admin func(sarama.Client) (sarama.ClusterAdmin, error),
) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "offsets",
		Short: "Report cluster offset status",
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := client()
			if err != nil {
				log.Error("Error initializing cluster client")
				return err
			}

			admin, err := admin(client)
			if err != nil {
				return fmt.Errorf("Error creating admin client: %v", err)
			}
			defer admin.Close() // closes underlying client

			return executeOffsetReport(client, admin)
		},
	}
	return cmd
}

// ConsumerLagRow represents a (group, topic, partition) 3-tuple for reporting
// consumer lag and will be tagged with the active consumer if one exists.
type ConsumerLagRow struct {
	Group           string
	Topic           string
	Partition       int32
	LatestOffset    int64
	CommittedOffset int64
	MemberId        string
	ClientHost      string
	ClientId        string
}

// executeOffsetReport calculates consumer lag for all consumer groups and
// reports the summary in table format.
func executeOffsetReport(
	client sarama.Client, admin sarama.ClusterAdmin,
) error {
	// get all groups and their current members
	groups, err := describeConsumerGroups(admin)
	if err != nil {
		return fmt.Errorf("Error retrieving group information: %v", err)
	}

	// collect consumer lag information for all groups/members
	var lastError error
	var consumers []ConsumerLagRow
	for _, groupDesc := range groups {
		c, err := getConsumerLag(client, admin, groupDesc)
		if err != nil {
			log.Warnf("Error querying consumers for group %s: %v",
				groupDesc.GroupId, err)
			lastError = err
			continue
		}
		consumers = append(consumers, c...)
	}
	if len(consumers) == 0 && lastError != nil {
		return fmt.Errorf("Error listing consumer groups: %v", err)
	}

	// group output by (group, topic, partition)
	sort.Slice(consumers, func(i, j int) bool {
		a := &consumers[i]
		b := &consumers[j]
		if a.Group != b.Group {
			return a.Group < b.Group
		}
		if a.Topic != b.Topic {
			return a.Topic < b.Topic
		}
		return a.Partition < b.Partition
	})

	t := ui.NewRpkTable(log.StandardLogger().Out)
	t.SetColWidth(80)
	t.SetAutoWrapText(false)

	header := [...]string{"Group", "Topic", "Partition", "Lag", "Lag %",
		"Committed", "Latest", "Consumer", "Client-Host", "Client-Id"}
	t.SetHeader(header[:])

	for _, consumer := range consumers {
		consumerId := "-"
		if consumer.MemberId != "" {
			consumerId = consumer.MemberId
		}
		lagStr := "-"
		lagPctStr := "-"
		committedOffset := "-"
		if consumer.CommittedOffset >= 0 {
			committedOffset = fmt.Sprintf("%d", consumer.CommittedOffset)
			lag := consumer.LatestOffset - consumer.CommittedOffset
			lagPct := 1.0 - (float64(consumer.CommittedOffset)+1)/
				(float64(consumer.LatestOffset)+1)
			if lagPct < 0 {
				lagPct = 0
			} else if lagPct > 1 {
				lagPct = 1
			}
			lagStr = fmt.Sprintf("%d", lag)
			lagPctStr = fmt.Sprintf("%f", 100.0*lagPct)
		}
		row := [...]string{
			consumer.Group,
			consumer.Topic,
			fmt.Sprintf("%d", consumer.Partition),
			lagStr,
			lagPctStr,
			committedOffset,
			fmt.Sprintf("%d", consumer.LatestOffset),
			consumerId,
			consumer.ClientHost,
			consumer.ClientId}
		t.Append(row[:])
	}

	t.Render()

	return nil
}

// describeConsumerGroups returns group description for all groups.
func describeConsumerGroups(
	admin sarama.ClusterAdmin,
) ([]*sarama.GroupDescription, error) {
	groups, err := admin.ListConsumerGroups()
	if err != nil {
		return nil, fmt.Errorf("Error listing consumer groups: %v", err)
	}

	groupIds := make([]string, 0, len(groups))
	for groupId := range groups {
		groupIds = append(groupIds, groupId)
	}

	descs, err := admin.DescribeConsumerGroups(groupIds)
	if err != nil {
		return nil, fmt.Errorf("Error describing groups: %v", err)
	}

	return descs, nil
}

type ConsumerLagRowMemberDesc struct {
	MemberId   string
	ClientHost string
	ClientId   string
	Matched    bool
}

// getConsumerLag computes lag for all topic partitions and members associated
// with the specified group.
func getConsumerLag(
	client sarama.Client,
	admin sarama.ClusterAdmin,
	groupDesc *sarama.GroupDescription,
) ([]ConsumerLagRow, error) {
	groupId := groupDesc.GroupId

	// get committed offset for each topic-partition in this group
	offsets, err := admin.ListConsumerGroupOffsets(groupId, nil)
	if err != nil {
		return nil, fmt.Errorf("Error fetching group offsets: %v", err)
	}

	// build a reverse lookup table so each topic-partition row that is
	// reported can be annotated with its consumer id if one exists
	memberAssignments := map[string]map[int32]ConsumerLagRowMemberDesc{}
	for memberId, memberDesc := range groupDesc.Members {
		assignments, err := memberDesc.GetMemberAssignment()
		if err != nil {
			return nil, fmt.Errorf("Error decoding assignments for "+
				"group %s: %v", groupId, err)
		}
		if len(assignments.Topics) == 0 {
			continue
		}
		for topic, partitions := range assignments.Topics {
			for _, partition := range partitions {
				if _, ok := memberAssignments[topic]; !ok {
					memberAssignments[topic] = map[int32]ConsumerLagRowMemberDesc{}
				}
				memberAssignments[topic][partition] = ConsumerLagRowMemberDesc{
					MemberId:   memberId,
					ClientHost: memberDesc.ClientHost,
					ClientId:   memberDesc.ClientId,
				}
			}
		}
	}

	// convert each group's topic-partition and committed offset into a consumer
	// lag row and annotate it with the partition's latest offset and the
	// consumer id of any group member assigned to the given partition.
	var consumers []ConsumerLagRow
	for topic, partitions := range offsets.Blocks {
		for partition, partitionInfo := range partitions {
			latestOffset, err := client.GetOffset(topic, partition, sarama.OffsetNewest)
			if err != nil {
				return nil, fmt.Errorf("Error listing consumer groups: %v", err)
			}
			row := ConsumerLagRow{
				Group:           groupId,
				Topic:           topic,
				Partition:       partition,
				LatestOffset:    latestOffset,
				CommittedOffset: partitionInfo.Offset,
			}
			if partitions, ok := memberAssignments[topic]; ok {
				if member, ok := partitions[partition]; ok {
					row.MemberId = member.MemberId
					row.ClientHost = member.ClientHost
					row.ClientId = member.ClientId
					member.Matched = true
				}
			}
			consumers = append(consumers, row)
		}
	}

	// Also add in members that didn't have a committed offset
	for topic := range memberAssignments {
		for partition := range memberAssignments[topic] {
			member := memberAssignments[topic][partition]
			if member.Matched {
				continue
			}
			latestOffset, err := client.GetOffset(topic, partition, sarama.OffsetNewest)
			if err != nil {
				return nil, fmt.Errorf("Error fetching partition offset: %v", err)
			}
			row := ConsumerLagRow{
				Group:           groupId,
				Topic:           topic,
				Partition:       partition,
				LatestOffset:    latestOffset,
				CommittedOffset: -1,
				MemberId:        member.MemberId,
				ClientHost:      member.ClientHost,
				ClientId:        member.ClientId,
			}
			consumers = append(consumers, row)
		}
	}

	return consumers, nil
}
