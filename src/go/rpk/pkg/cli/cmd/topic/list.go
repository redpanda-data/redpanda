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
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/cli/cmd/cluster"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/config"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/kafka"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/out"
)

func NewListCommand(fs afero.Fs) *cobra.Command {
	var detailed bool
	var internal bool

	cmd := &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List topics",
		Long:    `List topics (alias for rpk cluster metadata -t).`,
		Args:    cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, _ []string) {
			p := config.ParamsFromCommand(cmd)
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			cl, err := kafka.NewFranzClient(fs, cfg)
			out.MaybeDie(err, "unable to initialize kafka client: %v", err)
			defer cl.Close()

			req := kmsg.NewPtrMetadataRequest()
			resp, err := req.RequestWith(context.Background(), cl)
			out.MaybeDie(err, "unable to request metadata: %v", err)

			cluster.PrintTopics(resp.Topics, internal, detailed)
		},
	}

	cmd.Flags().BoolVarP(&detailed, "detailed", "d", false, "print per-partition information for topics")
	cmd.Flags().BoolVarP(&internal, "internal", "i", false, "print internal topics")
	return cmd
}
