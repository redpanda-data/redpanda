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

func NewDeleteCommand(fs afero.Fs) *cobra.Command {
	return &cobra.Command{
		Use:   "delete [TOPICS...]",
		Short: "Delete a topic",
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, topics []string) {
			p := config.ParamsFromCommand(cmd)
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			cl, err := kafka.NewFranzClient(fs, cfg)
			out.MaybeDie(err, "unable to initialize kafka client: %v", err)
			defer cl.Close()

			req := kmsg.NewPtrDeleteTopicsRequest()
			req.TimeoutMillis = 5000
			req.TopicNames = topics        // v0 thru v5
			for _, topic := range topics { // v6+
				t := kmsg.NewDeleteTopicsRequestTopic()
				t.Topic = kmsg.StringPtr(topic)
				req.Topics = append(req.Topics, t)
			}

			resp, err := req.RequestWith(context.Background(), cl)
			out.MaybeDie(err, "unable to issue delete topics request: %v", err)
			tw := out.NewTable("topic", "status")
			defer tw.Flush()
			for _, respTopic := range resp.Topics {
				msg := "OK"
				if err := kerr.ErrorForCode(respTopic.ErrorCode); err != nil {
					msg = err.Error()
				}
				// v6+ introduced topic IDs and allows the
				// response topic to be nil if we request with
				// IDs. We aren't, so we should have a topic.
				topic := "[BUG:empty!]"
				if respTopic.Topic != nil {
					topic = *respTopic.Topic
				}
				tw.Print(topic, msg)
			}
		},
	}
}
