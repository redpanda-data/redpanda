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
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/cli/cmd/cluster"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/config"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/kafka"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/out"
)

func NewListCommand(fs afero.Fs) *cobra.Command {
	var (
		detailed bool
		internal bool
		re       bool
	)
	cmd := &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List topics, optionally listing specific topics.",
		Long: `List topics, optionally listing specific topics.

This command lists all topics that you have access to by default. If specifying
topics or regular expressions, this command can be used to know exactly what
topics you would delete if using the same input to the delete command.

Alternatively, you can request specific topics to list, which can be used to
check authentication errors (do you not have access to a topic you were
expecting to see?), or to list all topics that match regular expressions.

The --regex flag (-r) opts into parsing the input topics as regular expressions
and listing any non-internal topic that matches any of expressions. The input
expressions are wrapped with ^ and $ so that the expression must match the
whole topic name. Regular expressions cannot be used to match internal topics,
as such, specifying both -i and -r will exit with failure.

Lastly, --detailed flag (-d) opts in to printing extra per-partition
information.
`,
		Run: func(cmd *cobra.Command, topics []string) {
			// The purpose of the regex flag really is for users to
			// know what topics they will delete when using regex.
			// We forbit deleting internal topics (redpanda
			// actually does not expose these currently), so we
			// make -r and -i incompatible.
			if internal && re {
				out.Exit("cannot list with internal topics and list by regular expression")
			}

			p := config.ParamsFromCommand(cmd)
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			adm, err := kafka.NewAdmin(fs, p, cfg)
			out.MaybeDie(err, "unable to initialize kafka client: %v", err)
			defer adm.Close()

			if re {
				topics, err = regexTopics(adm, topics)
				out.MaybeDie(err, "unable to filter topics by regex: %v", err)
			}

			listed, err := adm.ListInternalTopics(context.Background(), topics...)
			out.MaybeDie(err, "unable to request metadata: %v", err)
			cluster.PrintTopics(listed, internal, detailed)
		},
	}

	cmd.Flags().BoolVarP(&detailed, "detailed", "d", false, "print per-partition information for topics")
	cmd.Flags().BoolVarP(&internal, "internal", "i", false, "print internal topics")
	cmd.Flags().BoolVarP(&re, "regex", "r", false, "parse topics as regex; list any topic that matches any input topic expression")
	return cmd
}
