// Copyright 2021 Redpanda Data, Inc.
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
	"errors"
	"fmt"
	"os"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/kafka"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kerr"
	"go.uber.org/zap"
)

func newAddPartitionsCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var num int
	var force bool
	cmd := &cobra.Command{
		Use:   "add-partitions [TOPICS...] --num [#]",
		Short: "Add partitions to existing topics",
		Args:  cobra.MinimumNArgs(1),
		Long:  `Add partitions to existing topics.`,
		Run: func(_ *cobra.Command, topics []string) {
			if !force {
				for _, t := range topics {
					if t == "__consumer_offsets" || t == "_schemas" || t == "__transaction_state" || t == "coprocessor_internal_topic" {
						out.Exit("Unable to change %s without the --force flag.", t)
					}
				}
			}
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)

			adm, err := kafka.NewAdmin(fs, p)
			out.MaybeDie(err, "unable to initialize kafka client: %v", err)
			defer adm.Close()

			if num <= 0 {
				out.Die("--num (-n) should be a positive value, exiting!")
			}

			resps, err := adm.CreatePartitions(context.Background(), num, topics...)
			out.MaybeDie(err, "create partitions request failed: %v", err)

			var exit1 bool
			defer func() {
				if exit1 {
					os.Exit(1)
				}
			}()

			tw := out.NewTable("topic", "status")
			defer tw.Flush()

			for _, resp := range resps.Sorted() {
				msg := "OK"
				if resp.ErrMessage != "" {
					zap.L().Sugar().Debugf("redpanda returned error message: %v", resp.ErrMessage)
				}
				if e := resp.Err; e != nil {
					if errors.Is(e, kerr.InvalidPartitions) && num > 0 {
						msg = fmt.Sprintf("INVALID_PARTITIONS: unable to add %d partitions due to hardware constraints", num)
					} else {
						msg = e.Error()
						if ke := (*kerr.Error)(nil); errors.As(e, &ke) {
							if resp.ErrMessage != "" {
								msg = ke.Message + ": " + resp.ErrMessage
							}
						}
					}
					exit1 = true
				}
				tw.Print(resp.Topic, msg)
			}
		},
	}
	cmd.Flags().IntVarP(&num, "num", "n", 0, "Number of partitions to add to each topic")
	cmd.MarkFlagRequired("num")
	cmd.Flags().BoolVarP(&force, "force", "f", false, "Force change the partition count in internal topics, e.g. __consumer_offsets.")
	return cmd
}
