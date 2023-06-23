// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package config

import (
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func NewConfigCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var all bool
	cmd := &cobra.Command{
		Use:   "config",
		Args:  cobra.ExactArgs(0),
		Short: "Interact with cluster configuration properties",
		Long: `Interact with cluster configuration properties.

Cluster properties are redpanda settings which apply to all nodes in
the cluster.  These are separate to node properties, which are set with
'rpk redpanda config'.

Use the 'edit' subcommand to interactively modify the cluster configuration, or
'export' and 'import' to write configuration to a file that can be edited and
read back later.

These commands take an optional '--all' flag to include all properties including
low level tunables such as internal buffer sizes, that do not usually need
to be changed during normal operations.  These properties generally require
some expertize to set safely, so if in doubt, avoid using '--all'.

Modified properties are propagated immediately to all nodes.  The 'status'
subcommand can be used to verify that all nodes are up to date, and identify
any settings which were rejected by a node, for example if a node is running a
different redpanda version that does not recognize certain properties.`,
	}
	cmd.PersistentFlags().BoolVar(
		&all,
		"all",
		false,
		"Include all properties, including tunables",
	)
	p.InstallAdminFlags(cmd)
	p.InstallSASLFlags(cmd)

	cmd.AddCommand(
		newImportCommand(fs, p, &all),
		newExportCommand(fs, p, &all),
		newEditCommand(fs, p, &all),
		newStatusCommand(fs, p),
		newForceResetCommand(fs, p),
		newLintCommand(fs, p),
		newSetCommand(fs, p),
		newGetCommand(fs, p),
	)

	return cmd
}
