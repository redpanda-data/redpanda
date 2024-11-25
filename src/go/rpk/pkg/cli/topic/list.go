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

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cluster"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/kafka"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newListCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var (
		detailed bool
		internal bool
		re       bool
	)
	cmd := &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List topics, optionally listing specific topics",
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
		Run: func(_ *cobra.Command, topics []string) {
			// The purpose of the regex flag really is for users to
			// know what topics they will delete when using regex.
			// We forbid deleting internal topics (redpanda
			// actually does not expose these currently), so we
			// make -r and -i incompatible.
			if internal && re {
				out.Exit("cannot list with internal topics and list by regular expression")
			}

			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)

			adm, err := kafka.NewAdmin(fs, p)
			out.MaybeDie(err, "unable to initialize kafka client: %v", err)
			defer adm.Close()

			if re {
				topics, err = regexTopics(adm, topics)
				out.MaybeDie(err, "unable to filter topics by regex: %v", err)
			}

			listed, err := adm.ListTopicsWithInternal(context.Background(), topics...)
			out.MaybeDie(err, "unable to request metadata: %v", err)
			cluster.PrintTopics(listed, internal, detailed)
		},
	}

	cmd.Flags().BoolVarP(&detailed, "detailed", "d", false, "Print per-partition information for topics")
	cmd.Flags().BoolVarP(&internal, "internal", "i", false, "Print internal topics")
	cmd.Flags().BoolVarP(&re, "regex", "r", false, "Parse topics as regex; list any topic that matches any input topic expression")
	return cmd
}
