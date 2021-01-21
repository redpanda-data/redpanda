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
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/cli/cmd/common"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/cli/cmd/topic"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/cli/ui"
)

func NewTopicCommand(
	fs afero.Fs,
	client func() (sarama.Client, error),
	admin func() (sarama.ClusterAdmin, error),
) *cobra.Command {
	root := &cobra.Command{
		Use:	"topic",
		Short:	"Create, delete or update topics",
	}
	root.AddCommand(createTopic(admin))
	root.AddCommand(deleteTopic(admin))
	root.AddCommand(setTopicConfig(admin))
	root.AddCommand(listTopics(admin))
	root.AddCommand(describeTopic(client, admin))
	root.AddCommand(topicStatus(admin))

	return root
}

func createTopic(admin func() (sarama.ClusterAdmin, error)) *cobra.Command {
	return common.Deprecated(
		topic.NewCreateCommand(admin),
		"rpk topic create",
	)
}

func deleteTopic(admin func() (sarama.ClusterAdmin, error)) *cobra.Command {
	return common.Deprecated(
		topic.NewDeleteCommand(admin),
		"rpk topic delete",
	)
}

func setTopicConfig(admin func() (sarama.ClusterAdmin, error)) *cobra.Command {
	return common.Deprecated(
		topic.NewSetConfigCommand(admin),
		"rpk topic set-config",
	)
}

func describeTopic(
	client func() (sarama.Client, error),
	admin func() (sarama.ClusterAdmin, error),
) *cobra.Command {
	return common.Deprecated(
		topic.NewDescribeCommand(client, admin),
		"rpk topic describe",
	)
}

func topicStatus(admin func() (sarama.ClusterAdmin, error)) *cobra.Command {
	detailed := false
	cmd := &cobra.Command{
		Use:		"status <topic name>",
		Aliases:	[]string{"health"},
		Short:		"Show a topic's status - leader, replication, etc.",
		Args:		common.ExactArgs(1, "topic's name is missing."),
		// We don't want Cobra printing CLI usage help if the error isn't about CLI usage.
		SilenceUsage:	true,
		RunE: func(cmd *cobra.Command, args []string) error {
			containsID := func(ids []int32, id int32) bool {
				for _, i := range ids {
					if i == id {
						return true
					}
				}
				return false
			}

			adm, err := admin()
			if err != nil {
				log.Error("Couldn't initialize API admin")
				return err
			}
			defer adm.Close()

			topicName := args[0]
			topicDetails, err := adm.DescribeTopics([]string{topicName})
			if err != nil {
				log.Error("Couldn't get the topic details")
				return err
			}
			detail := topicDetails[0]
			if len(detail.Partitions) == 0 {
				return fmt.Errorf("Topic '%s' not found", topicName)
			}

			brokers, _, err := adm.DescribeCluster()
			if err != nil {
				log.Error("Couldn't get the cluster info")
				return err
			}
			brokerIDs := []int32{}
			for _, b := range brokers {
				brokerIDs = append(brokerIDs, b.ID())
			}

			t := ui.NewRpkTable(log.StandardLogger().Out)
			t.SetColWidth(80)
			t.SetAutoWrapText(true)
			t.AppendBulk([][]string{
				{"Name", detail.Name},
				{"Internal", fmt.Sprintf("%t", detail.IsInternal)},
				{"Partitions", strconv.Itoa(len(detail.Partitions))},
			})

			underReplicated := []int32{}
			unavailable := []int32{}
			for _, p := range detail.Partitions {
				if len(p.Isr) < len(p.Replicas) {
					underReplicated = append(
						underReplicated,
						p.ID,
					)
				}
				leaderIsLive := containsID(brokerIDs, p.Leader)
				if p.Leader < 0 || !leaderIsLive {
					unavailable = append(unavailable, p.ID)
				}
			}

			underRepldValue := "None"
			unavailableValue := "None"
			if len(underReplicated) > 0 {
				underRepldValue = formatPartitions(
					underReplicated,
					detailed,
				)
			}
			if len(unavailable) > 0 {
				unavailableValue = formatPartitions(
					unavailable,
					detailed,
				)
			}
			t.AppendBulk([][]string{
				{"Under-replicated partitions", underRepldValue},
				{"Unavailable partitions", unavailableValue},
			})
			t.Render()

			return nil
		},
	}
	cmd.Flags().BoolVar(
		&detailed,
		"detailed",
		false,
		"If enabled, will display detailed information",
	)
	return cmd
}

func listTopics(admin func() (sarama.ClusterAdmin, error)) *cobra.Command {
	cmd := &cobra.Command{
		Use:		"list",
		Aliases:	[]string{"ls"},
		Short:		"List topics",
		Args:		cobra.ExactArgs(0),
		// We don't want Cobra printing CLI usage help if the error isn't about CLI usage.
		SilenceUsage:	true,
		RunE: func(cmd *cobra.Command, args []string) error {
			adm, err := admin()
			if err != nil {
				log.Error("Couldn't initialize API admin")
				return err
			}
			defer adm.Close()

			topics, err := adm.ListTopics()
			if err != nil {
				return err
			}
			if len(topics) == 0 {
				log.Info("No topics found.")
				return nil
			}

			sortedTopics := make(
				[]struct {
					name	string
					sarama.TopicDetail
				}, len(topics))

			i := 0
			for name, topic := range topics {
				sortedTopics[i].name = name
				sortedTopics[i].TopicDetail = topic
				i++
			}

			sort.Slice(sortedTopics, func(i int, j int) bool {
				return sortedTopics[i].name < sortedTopics[j].name
			})

			t := ui.NewRpkTable(log.StandardLogger().Out)
			t.Append([]string{"Name", "Partitions", "Replicas"})

			for _, topic := range sortedTopics {
				t.Append([]string{
					topic.name,
					strconv.Itoa(int(topic.NumPartitions)),
					strconv.Itoa(int(topic.ReplicationFactor)),
				})
			}
			t.Render()
			return nil
		},
	}
	return cmd
}

func formatPartitions(parts []int32, detailed bool) string {
	if detailed {
		return formatInt32Slice(parts)
	}
	return strconv.Itoa(len(parts)) + " partitions"
}

func formatInt32Slice(xs []int32) string {
	ss := make([]string, 0, len(xs))
	for _, x := range xs {
		ss = append(ss, fmt.Sprintf("%d", x))
	}
	return strings.Join(ss, ", ")
}
