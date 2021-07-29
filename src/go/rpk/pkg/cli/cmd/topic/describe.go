// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package topic

import (
	"fmt"
	"math"
	"sort"
	"strconv"

	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/cli/cmd/common"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/cli/ui"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/kafka"
)

func NewDescribeCommand(
	client func() (sarama.Client, error),
	admin func() (sarama.ClusterAdmin, error),
) *cobra.Command {
	var (
		page              int
		pageSize          int
		includeWatermarks bool
	)
	cmd := &cobra.Command{
		Use:   "describe <topic>",
		Short: "Describe topic",
		Long:  "Describe a topic. Default values of the configuration are omitted.",
		Args:  common.ExactArgs(1, "topic's name is missing."),
		// We don't want Cobra printing CLI usage help if the error isn't about CLI usage.
		SilenceUsage: true,
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
				Type: sarama.TopicResource,
				Name: topicName,
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
			partitionHeaders := []string{"Partition", "Leader", "Replicas"}
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
				row := []string{
					strconv.Itoa(int(partition.ID)),
					strconv.Itoa(int(partition.Leader)),
					fmt.Sprintf("%v", sortedReplicas),
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
