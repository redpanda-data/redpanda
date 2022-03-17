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
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/kafka"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
)

func NewAddPartitionsCommand(fs afero.Fs) *cobra.Command {
	var num int
	cmd := &cobra.Command{
		Use:   "add-partitions [TOPICS...] --num [#]",
		Short: "Add partitions to existing topics.",
		Args:  cobra.MinimumNArgs(1),
		Long:  `Add partitions to existing topics.`,
		Run: func(cmd *cobra.Command, topics []string) {
			p := config.ParamsFromCommand(cmd)
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			adm, err := kafka.NewAdmin(fs, p, cfg)
			out.MaybeDie(err, "unable to initialize kafka client: %v", err)
			defer adm.Close()

			if num <= 0 {
				out.Die("No additional partitions requested, exiting!")
			}

			resps, err := adm.CreatePartitions(context.Background(), num, topics...)
			out.MaybeDie(err, "create partitions request failed: %v", err)

			tw := out.NewTable("topic", "error")
			defer tw.Flush()

			for _, resp := range resps.Sorted() {
				msg := "OK"
				if e := resp.Err; e != nil {
					msg = e.Error()
				}
				tw.Print(resp.Topic, msg)
			}
		},
	}
	cmd.Flags().IntVarP(&num, "num", "n", 0, "numer of partitions to add to each topic")
	return cmd
}
