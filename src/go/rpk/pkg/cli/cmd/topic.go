// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package cmd

import (
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cmd/common"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cmd/topic"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func NewTopicCommand(fs afero.Fs, mgr config.Manager) *cobra.Command {
	var (
		brokers        []string
		configFile     string
		user           string
		password       string
		mechanism      string
		enableTLS      bool
		certFile       string
		keyFile        string
		truststoreFile string
	)
	command := &cobra.Command{
		Use:   "topic",
		Short: "Create, delete, produce to and consume from Redpanda topics.",
	}

	common.AddKafkaFlags(command, &configFile, &user, &password, &mechanism, &enableTLS, &certFile, &keyFile, &truststoreFile, &brokers)

	command.AddCommand(
		topic.NewAddPartitionsCommand(fs),
		topic.NewAlterConfigCommand(fs),
		topic.NewConsumeCommand(fs),
		topic.NewCreateCommand(fs),
		topic.NewDeleteCommand(fs),
		topic.NewDescribeCommand(fs),
		topic.NewListCommand(fs),
		topic.NewProduceCommand(fs),
	)

	return command
}
