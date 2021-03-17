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
	"github.com/Shopify/sarama"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/cli/cmd/common"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/cli/cmd/topic"
)

func NewTopicCommand(
	fs afero.Fs,
	client func() (sarama.Client, error),
	admin func() (sarama.ClusterAdmin, error),
) *cobra.Command {
	root := &cobra.Command{
		Use:   "topic",
		Short: "Create, delete or update topics",
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
	return common.Deprecated(
		topic.NewInfoCommand(admin),
		"rpk topic info",
	)
}

func listTopics(admin func() (sarama.ClusterAdmin, error)) *cobra.Command {
	return common.Deprecated(
		topic.NewListCommand(admin),
		"rpk topic list",
	)
}
