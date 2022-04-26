// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// Package partitions contains commands to talk to the Redpanda's admin partitions
// endpoints.
package partitions

import (
	"context"
	"strconv"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/kafka"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
)

// NewCommand returns the partitions admin command.
func NewCommand(fs afero.Fs) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "partitions",
		Short: "View and configure Redpanda partitions through the admin listener.",
		Args:  cobra.ExactArgs(0),
	}
	cmd.AddCommand(
		newListCommand(fs),
	)
	return cmd
}

func newListCommand(fs afero.Fs) *cobra.Command {
	var (
		leaderOnly bool
	)
	cmd := &cobra.Command{
		Use:     "list [BROKER ID]",
		Aliases: []string{"ls"},
		Short:   "List the partitions in a broker in the cluster.",
		Args:    cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			brokerId, err := strconv.Atoi(args[0])
			out.MaybeDie(err, "invalid broker %s: %v", args[0], err)
			if brokerId < 0 {
				out.Die("invalid negative broker id %v", brokerId)
			}

			p := config.ParamsFromCommand(cmd)
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			adm, err := kafka.NewAdmin(fs, p, cfg)
			out.MaybeDie(err, "unable to initialize kafka client: %v", err)
			defer adm.Close()

			var m kadm.Metadata
			m, err = adm.Metadata(context.Background())
			out.MaybeDie(err, "unable to request metadata: %v", err)

			tw := out.NewTable("TOPIC", "PARTITION", "IS_LEADER")
			defer tw.Flush()

			for _, t := range m.Topics.Sorted() {
				for _, pt := range t.Partitions.Sorted() {
					for _, rs := range pt.Replicas {
						if int(rs) == brokerId {
							var isLeader bool
							if int(pt.Leader) == brokerId {
								isLeader = true
								tw.Print(t.Topic, pt.Partition, isLeader)
							}
							if !leaderOnly && !isLeader {
								tw.Print(t.Topic, pt.Partition, isLeader)
							}
						}
					}
				}
			}
		},
	}

	cmd.Flags().BoolVarP(&leaderOnly, "leader-only", "l", false, "print the partitions on broker which are leaders")

	return cmd
}
