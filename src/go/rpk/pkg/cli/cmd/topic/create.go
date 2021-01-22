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
	"sort"
	"strings"

	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/cli/cmd/common"
)

func NewCreateCommand(
	admin func() (sarama.ClusterAdmin, error),
) *cobra.Command {
	var (
		partitions	int32
		replicas	int16
		compact		bool
		config		[]string
	)
	cmd := &cobra.Command{
		Use:	"create <topic name>",
		Short:	"Create a topic",
		Args:	common.ExactArgs(1, "topic's name is missing."),
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
