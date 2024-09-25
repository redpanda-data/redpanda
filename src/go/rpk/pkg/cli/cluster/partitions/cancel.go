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
	"fmt"

	"github.com/redpanda-data/common-go/rpadmin"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

type movementCancelHandler struct {
	fs        afero.Fs
	p         *config.Params
	node      int
	noConfirm bool
}

func newMovementCancelCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	m := &movementCancelHandler{
		fs: fs,
		p:  p,
	}
	cmd := &cobra.Command{
		Use:     "move-cancel",
		Aliases: []string{"alter-cancel"},
		Short:   "Cancel ongoing partition movements",
		Long: `Cancel ongoing partition movements.

By default, this command cancels all the partition movements in the cluster. 
To ensure that you do not accidentally cancel all partition movements, this 
command prompts users for confirmation before issuing the cancellation request. 
You can use "--no-confirm" to disable the confirmation prompt:

    rpk cluster partitions move-cancel --no-confirm

If "--node" is set, this command will only stop the partition movements 
occurring in the specified node:

    rpk cluster partitions move-cancel --node 1
`,
		Args: cobra.ExactArgs(0),
		Run:  m.runMovementCancel,
	}
	cmd.Flags().IntVar(&m.node, "node", -1, "ID of a specific node on which to cancel ongoing partition movements")
	cmd.Flags().BoolVar(&m.noConfirm, "no-confirm", false, "Disable confirmation prompt")
	return cmd
}

func (m *movementCancelHandler) runMovementCancel(cmd *cobra.Command, _ []string) {
	p, err := m.p.LoadVirtualProfile(m.fs)
	out.MaybeDie(err, "rpk unable to load config: %v", err)
	config.CheckExitCloudAdmin(p)

	cl, err := adminapi.NewClient(cmd.Context(), m.fs, p)
	out.MaybeDie(err, "unable to initialize admin client: %v", err)

	var movements []rpadmin.PartitionsMovementResult
	if m.node >= 0 {
		if !m.noConfirm {
			confirmed, err := out.Confirm("Confirm cancellation of partition movements in node %v?", m.node)
			out.MaybeDie(err, "unable to confirm partition movements cancel: %v", err)
			if !confirmed {
				out.Exit("Command execution canceled.")
			}
		}
		movements, err = cl.CancelNodePartitionsMovement(cmd.Context(), m.node)
		out.MaybeDie(err, "unable to cancel partition movements in node %v: %v", m.node, err)
	} else {
		if !m.noConfirm {
			confirmed, err := out.Confirm("Confirm cancellation of all partition movements in the cluster?")
			out.MaybeDie(err, "unable to confirm partition movements cancel: %v", err)
			if !confirmed {
				out.Exit("Command execution canceled.")
			}
		}
		movements, err = cl.CancelAllPartitionsMovement(cmd.Context())
		out.MaybeDie(err, "unable to cancel partition movements: %v", err)
	}

	if len(movements) == 0 {
		fmt.Println("There are no ongoing partition movements to cancel")
		return
	}
	printMovementsResult(movements)
}

func printMovementsResult(movements []rpadmin.PartitionsMovementResult) {
	headers := []string{
		"NAMESPACE",
		"TOPIC",
		"PARTITION",
		"RESULT",
	}
	tw := out.NewTable(headers...)
	defer tw.Flush()
	for _, m := range movements {
		result := struct {
			Namespace string
			Topic     string
			Partition int
			Result    string
		}{m.Namespace, m.Topic, m.Partition, m.Result}
		tw.PrintStructFields(result)
	}
}
