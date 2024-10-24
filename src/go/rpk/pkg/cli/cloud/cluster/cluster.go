// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package cluster

import (
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func NewCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cluster",
		Short: "Manage rpk cloud clusters",
		Long: `Manage rpk cloud clusters.

This command allows you to manage cloud clusters, as well as easily switch
between which cluster you are talking to.
`,
	}

	cmd.AddCommand(
		newSelectCommand(fs, p),
	)

	return cmd
}
