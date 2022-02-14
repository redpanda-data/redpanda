// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package config

import (
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/cli/cmd/common"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/config"
)

func NewConfigCommand(fs afero.Fs) *cobra.Command {
	var (
		all            bool
		adminURL       string
		adminEnableTLS bool
		adminCertFile  string
		adminKeyFile   string
		adminCAFile    string
	)

	command := &cobra.Command{
		Use:   "config",
		Args:  cobra.ExactArgs(0),
		Short: "Interact with cluster configuration properties.",
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

	command.PersistentFlags().StringVar(
		&adminURL,
		config.FlagAdminHosts2,
		"",
		"Comma-separated list of admin API addresses (<IP>:<port>")

	common.AddAdminAPITLSFlags(command,
		&adminEnableTLS,
		&adminCertFile,
		&adminKeyFile,
		&adminCAFile,
	)

	command.PersistentFlags().BoolVar(
		&all,
		"all",
		false,
		"Include all properties, including tunables.",
	)

	command.AddCommand(
		newImportCommand(fs, &all),
		newExportCommand(fs, &all),
		newEditCommand(fs, &all),
		newStatusCommand(fs),
		newResetCommand(fs),
		newLintCommand(fs),
		newSetCommand(fs),
		newGetCommand(fs),
	)

	return command
}
