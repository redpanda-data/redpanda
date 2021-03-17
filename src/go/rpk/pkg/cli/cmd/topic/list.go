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
	"sort"
	"strconv"

	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/cli/ui"
)

func NewListCommand(admin func() (sarama.ClusterAdmin, error)) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List topics",
		Args:    cobra.ExactArgs(0),
		// We don't want Cobra printing CLI usage help if the error isn't about CLI usage.
		SilenceUsage: true,
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
					name string
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
