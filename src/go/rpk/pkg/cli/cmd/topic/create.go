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
	"context"

	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/config"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/kafka"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/out"
)

func NewCreateCommand(fs afero.Fs) *cobra.Command {
	var (
		dry        bool
		partitions int32
		replicas   int16
		compact    bool
		configKVs  []string
	)
	cmd := &cobra.Command{
		Use:   "create [TOPICS...]",
		Short: "Create topics.",
		Args:  cobra.MinimumNArgs(1),
		Long: `Create topics.

All topics created with this command will have the same number of partitions,
replication factor, and key/value configs.

For example,

	create -c cleanup.policy=compact -r 3 -p 20 foo bar

will create two topics, foo and bar, each with 20 partitions, 3 replicas, and
the cleanup.policy=compact config option set.
`,

		Run: func(cmd *cobra.Command, topics []string) {
			p := config.ParamsFromCommand(cmd)
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			cl, err := kafka.NewFranzClient(fs, p, cfg)
			out.MaybeDie(err, "unable to initialize kafka client: %v", err)
			defer cl.Close()

			configs, err := parseKVs(configKVs)
			out.MaybeDie(err, "unable to parse configs: %v", err)
			if compact {
				if _, ok := configs["cleanup.policy"]; !ok {
					configs["cleanup.policy"] = "compact"
				}
			}

			req := kmsg.NewPtrCreateTopicsRequest()
			req.ValidateOnly = dry
			req.TimeoutMillis = 5000 // TODO move to rpk.kafka
			var reqConfigs []kmsg.CreateTopicsRequestTopicConfig
			for k, v := range configs {
				reqConfig := kmsg.NewCreateTopicsRequestTopicConfig()
				reqConfig.Name = k
				reqConfig.Value = kmsg.StringPtr(v)
				reqConfigs = append(reqConfigs, reqConfig)
			}
			for _, topic := range topics {
				reqTopic := kmsg.NewCreateTopicsRequestTopic()
				reqTopic.Topic = topic
				reqTopic.ReplicationFactor = replicas
				reqTopic.NumPartitions = partitions
				reqTopic.Configs = reqConfigs
				req.Topics = append(req.Topics, reqTopic)
			}

			resp, err := req.RequestWith(context.Background(), cl)
			out.MaybeDie(err, "unable to create topics %v: %v", topics, err)

			tw := out.NewTable("topic", "status")
			defer tw.Flush()

			for _, topic := range resp.Topics {
				msg := "OK"
				if err := kerr.ErrorForCode(topic.ErrorCode); err != nil {
					msg = err.Error()
				}
				tw.Print(topic.Topic, msg)
			}
		},
	}
	cmd.Flags().StringArrayVarP(&configKVs, "topic-config", "c", nil, "key=value; Config parameters (repeatable; e.g. -c cleanup.policy=compact)")
	cmd.Flags().Int32VarP(&partitions, "partitions", "p", 1, "Number of partitions to create per topic")
	cmd.Flags().Int16VarP(&replicas, "replicas", "r", -1, "Replication factor; if -1, this will be the broker's default.replication.factor")
	cmd.Flags().BoolVarP(&dry, "dry", "d", false, "dry run: validate the topic creation request; do not create topics")

	// Sept 2021
	cmd.Flags().BoolVar(&compact, "compact", false, "alias for -c cleanup.policy=compact")
	cmd.Flags().MarkDeprecated("compact", "use -c cleanup.policy=compact")

	return cmd
}
