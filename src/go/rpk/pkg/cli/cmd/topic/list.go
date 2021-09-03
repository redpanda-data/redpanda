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
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/cli/cmd/cluster"
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
		Run: func(listCmd *cobra.Command, _ []string) {
			args := append(listCmd.Flags().Args(), "-t")
			if detailed {
				args = append(args, "-d")
			}
			if internal {
				args = append(args, "-i")
			}
			metaCmd := cluster.NewMetadataCommand(fs)
			metaCmd.SetArgs(args)
			metaCmd.Execute()
		},
	}

	cmd.Flags().BoolVarP(&detailed, "detailed", "d", false, "print per-partition information for topics")
	cmd.Flags().BoolVarP(&internal, "internal", "i", false, "print internal topics")
	return cmd
}
