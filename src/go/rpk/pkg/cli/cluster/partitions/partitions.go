// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package partitions

import (
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func NewPartitionsCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "partitions",
		Args:  cobra.ExactArgs(0),
		Short: "Manage cluster partitions",
	}
	p.InstallAdminFlags(cmd)
	p.InstallSASLFlags(cmd)
	cmd.AddCommand(
		newMovePartitionReplicasCommand(fs, p),
		newBalancerStatusCommand(fs, p),
		newListCommand(fs, p),
		newMovementCancelCommand(fs, p),
		newMovementCancelCommandHidden(fs, p),
		newPartitionDisableCommand(fs, p),
		newPartitionEnableCommand(fs, p),
		newPartitionMovementsStatusCommand(fs, p),
		newTransferLeaderCommand(fs, p),
		newTriggerBalancerCommand(fs, p),
		newUnsafeRecoveryCommand(fs, p),
	)
	return cmd
}
