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
	"errors"
	"fmt"
	"strings"

	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

func NewSetConfigCommand(
	admin func() (sarama.ClusterAdmin, error),
) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "set-config <topic> <key>=<value> [<key>=<value>...]",
		Short: "Set the topic's config key/value pairs",
		Args: func(_ *cobra.Command, args []string) error {
			if len(args) < 2 {
				return errors.New("a topic name and at least one key=value pair is required")
			}
			return nil
		},
		// We don't want Cobra printing CLI usage help if the error isn't about CLI usage.
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {

			topicName := args[0]

			kvs := map[string]*string{}
			for _, a := range args[1:] {
				kv := strings.Split(a, "=")
				if len(kv) != 2 {
					return fmt.Errorf("invalid element '%s'. Expected format <key>=<value>", a)
				}
				kvs[kv[0]] = &kv[1]
			}

			adm, err := admin()
			if err != nil {
				log.Error("Couldn't initialize API admin")
				return err
			}
			defer adm.Close()

			err = adm.AlterConfig(
				sarama.TopicResource,
				topicName,
				kvs,
				false,
			)
			if err != nil {
				return err
			}
			log.Infof(
				"Added configs %s to topic '%s'.",
				strings.Join(args[1:], ", "),
				topicName,
			)
			return nil
		},
	}
	return cmd
}
