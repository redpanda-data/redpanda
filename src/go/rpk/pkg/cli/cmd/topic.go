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
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/cli/cmd/common"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/cli/cmd/topic"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/config"
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

	command.AddCommand(topic.NewCreateCommand(fs))
	command.AddCommand(topic.NewDeleteCommand(fs))
	command.AddCommand(topic.NewAlterConfigCommand(fs))
	command.AddCommand(topic.NewDescribeCommand(fs))
	command.AddCommand(topic.NewListCommand(fs))
	command.AddCommand(topic.NewConsumeCommand(fs))
	command.AddCommand(topic.NewProduceCommand(fs))

	return command
}
