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

func newMovementCancelCommandHidden(fs afero.Fs, p *config.Params) *cobra.Command {
	m := &movementCancelHandler{
		fs: fs,
		p:  p,
	}
	cmd := &cobra.Command{
		Use:    "movement-cancel",
		Short:  "Cancel ongoing partition movements",
		Hidden: true,
		Long: `Cancel ongoing partition movements.

By default, this command cancels all the partition movements in the cluster.
To ensure that you do not accidentally cancel all partition movements, this
command prompts users for confirmation before issuing the cancellation request.
You can use "--no-confirm" to disable the confirmation prompt:

    rpk cluster partitions movement-cancel --no-confirm

If "--node" is set, this command will only stop the partition movements
occurring in the specified node:

    rpk cluster partitions movement-cancel --node 1
`,
		Args: cobra.ExactArgs(0),
		Run:  m.runMovementCancel,
	}
	cmd.Flags().IntVar(&m.node, "node", -1, "ID of a specific node on which to cancel ongoing partition movements")
	cmd.Flags().BoolVar(&m.noConfirm, "no-confirm", false, "Disable confirmation prompt")
	return cmd
}
