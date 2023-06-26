// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package maintenance

import (
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func NewMaintenanceCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "maintenance",
		Short: "Toggle a node's maintenance mode",
		Long: `Interact with cluster maintenance mode.

Maintenance mode is a state that a node may be placed into in which the node
may be shutdown or restarted with minimal disruption to client workloads. The
primary use of maintenance mode is to perform a rolling upgrade in which each
node is placed into maintenance mode prior to upgrading the node.

Use the 'enable' and 'disable' subcommands to place a node into maintenance mode
or remove it, respectively. Only one node at a time may be in maintenance mode.

When a node is placed into maintenance mode the following occurs:

Leadership draining. All raft leadership is transferred to another eligible
node, and the node in maintenance mode rejects new leadership requests. By
transferring leadership off of the node in maintenance mode all client traffic
and requests are directed to other nodes minimizing disruption to client
workloads when the node is shutdown.

Currently leadership is not transferred for partitions with one replica.
`,
	}
	p.InstallAdminFlags(cmd)
	p.InstallSASLFlags(cmd)
	cmd.AddCommand(
		newEnableCommand(fs, p),
		newDisableCommand(fs, p),
		newStatusCommand(fs, p))

	return cmd
}
