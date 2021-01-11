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
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/cli/ui"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/kafka"
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
	var (
		partitions	int32
		replicas	int16
		compact		bool
		config		[]string
	)
	cmd := &cobra.Command{
		Use:	"create <topic name>",
		Short:	"Create a topic",
		Args:	exactArgs(1, "topic's name is missing."),
		// We don't want Cobra printing CLI usage help if the error isn't about CLI usage.
		SilenceUsage:	true,
		RunE: func(cmd *cobra.Command, args []string) error {
			configEntries, err := parseKVs(config)
			if err != nil {
				return err
			}
			if _, ok := configEntries["cleanup.policy"]; !ok {
				cleanupPolicy := "delete"
				if compact {
					cleanupPolicy = "compact"
				}
				configEntries["cleanup.policy"] = &cleanupPolicy
			}
			adm, err := admin()
			if err != nil {
				log.Error("Couldn't initialize API admin")
				return err
			}
			defer adm.Close()
			topicName := args[0]
			topicDetail := &sarama.TopicDetail{
				NumPartitions:	partitions,
				ConfigEntries:	configEntries,
			}
			if replicas > 0 {
				topicDetail.ReplicationFactor = replicas
			}
			err = adm.CreateTopic(
				topicName,
				topicDetail,
				false,
			)
			if err != nil {
				return err
			}
			configList := []string{}
			for k, v := range configEntries {
				configList = append(
					configList,
					fmt.Sprintf("'%s':'%s'", k, *v),
				)
			}
			sort.Strings(configList)
			log.Infof(
				"Created topic '%s'. Partitions: %d,"+
					" replicas: %d, configuration:\n%s",
				topicName,
				partitions,
				replicas,
				strings.Join(configList, "\n"),
			)
			return nil
		},
	}
	cmd.Flags().StringArrayVarP(
		&config,
		"config",
		"c",
		[]string{},
		"Config entries in the format <key>:<value>. May be used multiple times"+
			" to add more entries.",
	)
	cmd.Flags().Int32VarP(
		&partitions,
		"partitions",
		"p",
		int32(1),
		"Number of partitions",
	)
	cmd.Flags().Int16VarP(
		&replicas,
		"replicas",
		"r",
		int16(-1),
		"Replication factor. If it's negative or is left unspecified,"+
			" it will use the cluster's default topic replication"+
			" factor.",
	)
	cmd.Flags().BoolVar(
		&compact,
		"compact",
		false,
		"Enable topic compaction",
	)
	return cmd
}

func deleteTopic(admin func() (sarama.ClusterAdmin, error)) *cobra.Command {
	cmd := &cobra.Command{
		Use:	"delete <topic name>",
		Short:	"Delete a topic",
		Args:	exactArgs(1, "topic's name is missing."),
		// We don't want Cobra printing CLI usage help if the error isn't about CLI usage.
		SilenceUsage:	true,
		RunE: func(cmd *cobra.Command, args []string) error {
			adm, err := admin()
			if err != nil {
				log.Error("Couldn't initialize API admin")
				return err
			}
			defer adm.Close()

			topicName := args[0]
			err = adm.DeleteTopic(topicName)
			if err != nil {
				return err
			}
			log.Infof("Deleted topic '%s'.", topicName)
			return nil
		},
	}
	return cmd
}

func setTopicConfig(admin func() (sarama.ClusterAdmin, error)) *cobra.Command {
	cmd := &cobra.Command{
		Use:	"set-config <topic> <key> <value>",
		Short:	"Set the topic's config key/value pairs",
		Args:	exactArgs(3, "topic's name, config key or value are missing."),
		// We don't want Cobra printing CLI usage help if the error isn't about CLI usage.
		SilenceUsage:	true,
		RunE: func(cmd *cobra.Command, args []string) error {
			adm, err := admin()
			if err != nil {
				log.Error("Couldn't initialize API admin")
				return err
			}
			defer adm.Close()

			topicName := args[0]
			key := args[1]
			value := args[2]

			err = adm.AlterConfig(
				sarama.TopicResource,
				topicName,
				map[string]*string{key: &value},
				false,
			)
			if err != nil {
				return err
			}
			log.Infof(
				"Added config '%s'='%s' to topic '%s'.",
				key,
				value,
				topicName,
			)
			return nil
		},
	}
	return cmd
}

func describeTopic(
	client func() (sarama.Client, error),
	admin func() (sarama.ClusterAdmin, error),
) *cobra.Command {
	var (
		page			int
		pageSize		int
		includeWatermarks	bool
	)
	cmd := &cobra.Command{
		Use:	"describe <topic>",
		Short:	"Describe topic",
		Long:	"Describe a topic. Default values of the configuration are omitted.",
		Args:	exactArgs(1, "topic's name is missing."),
		// We don't want Cobra printing CLI usage help if the error isn't about CLI usage.
		SilenceUsage:	true,
		RunE: func(cmd *cobra.Command, args []string) error {
			cl, err := client()
			if err != nil {
				log.Error("Couldn't initialize API client")
				return err
			}
			defer cl.Close()

			adm, err := admin()
			if err != nil {
				log.Error("Couldn't initialize API admin")
				return err
			}
			defer adm.Close()

			topicName := args[0]
			highWatermarks := map[int32]int64{}
			topicDetails, err := adm.DescribeTopics([]string{topicName})
			if err != nil {
				return err
			}
			detail := topicDetails[0]

			if detail.Err == sarama.ErrUnknownTopicOrPartition {
				return fmt.Errorf("topic '%v' not found", topicName)
			}

			cfg, err := adm.DescribeConfig(sarama.ConfigResource{
				Type:	sarama.TopicResource,
				Name:	topicName,
			})
			if err != nil {
				return err
			}

			cleanupPolicy := ""
			nonDefaultCfg := []sarama.ConfigEntry{}
			for _, e := range cfg {
				if e.Name == "cleanup.policy" {
					cleanupPolicy = e.Value
				}
				if !e.Default {
					nonDefaultCfg = append(nonDefaultCfg, e)
				}
			}

			sort.Slice(detail.Partitions, func(i, j int) bool {
				return detail.Partitions[i].ID < detail.Partitions[j].ID
			})

			t := ui.NewRpkTable(log.StandardLogger().Out)
			t.SetColWidth(80)
			t.SetAutoWrapText(true)
			t.AppendBulk([][]string{
				{"Name", detail.Name},
				{"Internal", fmt.Sprintf("%t", detail.IsInternal)},
			})
			if cleanupPolicy != "" {
				t.Append([]string{"Cleanup policy", cleanupPolicy})
			}

			if len(nonDefaultCfg) > 0 {
				t.Append([]string{"Config:"})
				t.Append([]string{"Name", "Value", "Read-only", "Sensitive"})
			}
			for _, entry := range nonDefaultCfg {
				t.Append([]string{
					entry.Name,
					entry.Value,
					strconv.FormatBool(entry.ReadOnly),
					strconv.FormatBool(entry.Sensitive),
				})
			}
			t.Render()
			t.ClearRows()

			pagedPartitions := detail.Partitions
			beginning := 0
			end := len(detail.Partitions)
			if page >= 0 {
				pagedPartitions, beginning, end = pagePartitions(
					detail.Partitions,
					page,
					pageSize,
				)
			}

			t.Append([]string{
				"Partitions",
				fmt.Sprintf(
					"%d - %d out of %d",
					beginning+1,
					end,
					len(detail.Partitions),
				),
			})
			t.Render()
			t.ClearRows()
			partitionHeaders := []string{"Partition", "Leader", "Replicas", "In-Sync Replicas"}
			if includeWatermarks {
				partitionHeaders = append(partitionHeaders, "High Watermark")
			}
			t.Append(partitionHeaders)

			partitions := make([]int32, 0, len(pagedPartitions))
			for _, partition := range pagedPartitions {
				partitions = append(partitions, partition.ID)
			}
			if includeWatermarks {
				highWatermarks, err = kafka.HighWatermarks(cl, topicName, partitions)
				if err != nil {
					return err
				}
			}

			for _, partition := range pagedPartitions {
				sortedReplicas := partition.Replicas
				sort.Slice(sortedReplicas, func(i, j int) bool {
					return sortedReplicas[i] < sortedReplicas[j]
				})

				sortedISR := partition.Isr
				sort.Slice(sortedISR, func(i, j int) bool {
					return sortedISR[i] < sortedISR[j]
				})
				row := []string{
					strconv.Itoa(int(partition.ID)),
					strconv.Itoa(int(partition.Leader)),
					fmt.Sprintf("%v", sortedReplicas),
					fmt.Sprintf("%v", sortedISR),
				}
				if includeWatermarks {
					row = append(
						row,
						strconv.Itoa(int(highWatermarks[partition.ID])),
					)
				}
				t.Append(row)
			}
			t.Render()
			return nil
		},
	}
	cmd.Flags().IntVar(
		&page,
		"page",
		-1,
		"The partitions page to display. If negative, all partitions will be shown",
	)
	cmd.Flags().IntVar(
		&pageSize,
		"page-size",
		20,
		"The number of partitions displayed per page",
	)
	cmd.Flags().BoolVar(
		&includeWatermarks,
		"watermarks",
		true,
		"If enabled, will display the topic's partitions' high watermarks",
	)
	return cmd
}

func topicStatus(admin func() (sarama.ClusterAdmin, error)) *cobra.Command {
	detailed := false
	cmd := &cobra.Command{
		Use:		"status <topic name>",
		Aliases:	[]string{"health"},
		Short:		"Show a topic's status - leader, replication, etc.",
		Args:		exactArgs(1, "topic's name is missing."),
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

func pagePartitions(
	parts []*sarama.PartitionMetadata, page int, pageSize int,
) ([]*sarama.PartitionMetadata, int, int) {
	noParts := len(parts)
	// If the number of partitions is less than the page size,
	// return all the partitions
	if noParts < pageSize {
		return parts, 0, noParts
	}
	noPages := noParts / pageSize
	beginning := page * pageSize
	// If the given page exceeds the number of pages,
	// or the beginning index exceeds the number of partitions,
	// return the last page
	if page > noPages || beginning >= noParts {
		lastPageSize := noParts - noPages*pageSize
		if lastPageSize == 0 {
			lastPageSize = pageSize
		}
		beginning = noParts - lastPageSize
		return parts[beginning:], beginning, noParts
	}
	end := int(math.Min(float64(beginning+pageSize), float64(noParts)))
	return parts[beginning:end], beginning, end
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
