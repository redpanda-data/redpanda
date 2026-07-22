// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package sql

import (
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/sql/debug"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

// NewCommand returns the `rpk sql` command group for interacting with a
// Redpanda SQL cluster.
func NewCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "sql",
		Short: "Interact with a Redpanda SQL cluster",
	}
	cmd.AddCommand(
		debug.NewCommand(fs, p),
	)
	return cmd
}
