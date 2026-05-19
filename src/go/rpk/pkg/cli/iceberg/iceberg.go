// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// Package iceberg implements `rpk iceberg`, a command group for validating
// and configuring the cluster's Iceberg catalog connection.
package iceberg

import (
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

// NewCommand returns the `rpk iceberg` command group.
func NewCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "iceberg",
		Short: "Validate and configure the cluster's Iceberg catalog",
		Long: `Validate and configure the cluster's Iceberg catalog.

This command group helps operators set up and troubleshoot the Iceberg
catalog connection. Use 'rpk iceberg test' to probe the currently-applied
catalog config, or to validate a proposed config without applying it (with
'--set key=value' overrides). Use 'rpk iceberg configure' for a guided
catalog-specific setup flow.`,
		Args: cobra.NoArgs,
	}
	cmd.AddCommand(
		newTestCommand(fs, p),
		newConfigureCommand(fs, p),
	)
	return cmd
}
