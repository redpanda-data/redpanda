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

	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/cli/cmd/common"
)

func NewSetConfigCommand(
	admin func() (sarama.ClusterAdmin, error),
) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "set-config <topic> <key> <value>",
		Short: "Set the topic's config key/value pairs",
		Args:  common.ExactArgs(3, "topic's name, config key or value are missing."),
		// We don't want Cobra printing CLI usage help if the error isn't about CLI usage.
		SilenceUsage:       true,
		DisableFlagParsing: true,
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

			res := sarama.ConfigResource{
				Type: sarama.TopicResource,
				Name: topicName,
			}
			configEntries, err := adm.DescribeConfig(res)

			newConfig := map[string]*string{}
			for _, configEntry := range configEntries {
				ce := configEntry
				if ce.ReadOnly || isReadOnly(ce.Name) {
					if ce.Name == key {
						return fmt.Errorf("property '%s' is read-only and cannot be modified", key)
					}
					continue
				}
				// Update the existing config with the new key-value pair.
				newConfig[ce.Name] = &ce.Value
			}

			newConfig[key] = &value

			err = adm.AlterConfig(
				sarama.TopicResource,
				topicName,
				newConfig,
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

// Some config properties are read-only, even if they're not marked as such
// in the response. isReadOnly checks the well known ones.
func isReadOnly(propertyName string) bool {
	readOnly := []string{"partition_count", "replication_factor"}
	for _, s := range readOnly {
		if propertyName == s {
			return true
		}
	}
	return false
}
