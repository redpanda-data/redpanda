// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package debug

import (
	"time"

	"github.com/spf13/cobra"
)

func NewInfoCommand() *cobra.Command {
	var (
		send    bool
		timeout time.Duration
	)
	cmd := &cobra.Command{
		Use:     "info",
		Short:   "Send usage stats to Redpanda Data",
		Hidden:  true,
		Aliases: []string{"status"},
		Args:    cobra.ExactArgs(0),
		Run: func(_ *cobra.Command, _ []string) {
			// no-op: keeping the command for backompat.
		},
	}
	cmd.Flags().BoolVar(&send, "send", false, "If true, send resource usage data to Redpanda")
	cmd.Flags().DurationVar(&timeout, "timeout", 2*time.Second, "How long to wait to calculate the Redpanda CPU % utilization")
	cmd.Flags().MarkHidden("send")
	cmd.Flags().MarkHidden("timeout")
	return cmd
}
