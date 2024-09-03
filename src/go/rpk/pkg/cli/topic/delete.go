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

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/kafka"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kerr"
	"go.uber.org/zap"
)

func newDeleteCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var re bool
	cmd := &cobra.Command{
		Use:   "delete [TOPICS...]",
		Short: "Delete topics",
		Long: `Delete topics.

This command deletes all requested topics, printing the success or fail status
per topic.

The --regex flag (-r) opts into parsing the input topics as regular expressions
and deleting any non-internal topic that matches any of expressions. The input
expressions are wrapped with ^ and $ so that the expression must match the
whole topic name (which also prevents accidental delete-everything mistakes).

The topic list command accepts the same input regex format as this delete
command. If you want to check what your regular expressions will delete before
actually deleting them, you can check the output of 'rpk topic list -r'.

For example,

    delete foo bar            # deletes topics foo and bar
    delete -r '^f.*' '.*r$'   # deletes any topic starting with f and any topics ending in r
    delete -r '.*'            # deletes all topics
    delete -r .               # deletes any one-character topics

`,

		Args: cobra.MinimumNArgs(1),
		Run: func(_ *cobra.Command, topics []string) {
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)

			adm, err := kafka.NewAdmin(fs, p)
			out.MaybeDie(err, "unable to initialize kafka client: %v", err)
			defer adm.Close()

			if re {
				topics, err = regexTopics(adm, topics)
				out.MaybeDie(err, "unable to filter topics by regex: %v", err)
			}

			resps, err := adm.DeleteTopics(context.Background(), topics...)
			out.MaybeDie(err, "unable to issue delete topics request: %v", err)
			tw := out.NewTable("topic", "status")
			defer tw.Flush()
			for _, t := range resps.Sorted() {
				msg := "OK"
				if t.Err != nil {
					msg = t.Err.Error()
					if t.ErrMessage != "" {
						zap.L().Sugar().Debugf("redpanda returned error message: %v", t.ErrMessage)
						if ke := (*kerr.Error)(nil); errors.As(t.Err, &ke) {
							msg = ke.Message + ": " + t.ErrMessage
						}
					}
				}
				tw.Print(t.Topic, msg)
			}
		},
	}
	cmd.Flags().BoolVarP(&re, "regex", "r", false, "Parse topics as regex; delete any topic that matches any input topic expression")
	return cmd
}
