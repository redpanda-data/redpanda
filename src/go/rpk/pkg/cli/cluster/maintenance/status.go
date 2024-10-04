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
	"fmt"

	"github.com/redpanda-data/common-go/rpadmin"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newMaintenanceReportTable() *out.TabWriter {
	headers := []string{
		"Node-ID", "Enabled", "Finished", "Errors",
		"Partitions", "Eligible", "Transferring", "Failed",
	}
	return out.NewTable(headers...)
}

func nullableToStr[V any](v *V) string {
	if v == nil {
		return "-"
	}

	return fmt.Sprint(*v)
}

func addBrokerMaintenanceReport(table *out.TabWriter, b rpadmin.Broker) {
	table.Print(
		b.NodeID,
		b.Maintenance.Draining,
		nullableToStr(b.Maintenance.Finished),
		nullableToStr(b.Maintenance.Errors),
		nullableToStr(b.Maintenance.Partitions),
		nullableToStr(b.Maintenance.Eligible),
		nullableToStr(b.Maintenance.Transferring),
		nullableToStr(b.Maintenance.Failed))
}

func newStatusCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Report maintenance status",
		Long: `Report maintenance status.

This command reports maintenance status for each node in the cluster. The output
is presented as a table with each row representing a node in the cluster.  The
output can be used to monitor the progress of node draining.

   NODE-ID  ENABLED  FINISHED  ERRORS  PARTITIONS  ELIGIBLE  TRANSFERRING  FAILED
   1        false     false     false   0           0         0             0

Field descriptions:

        NODE-ID: the node ID
        ENABLED: true if the node is currently in maintenance mode
       FINISHED: leadership draining has completed
         ERRORS: errors have been encountered while draining
     PARTITIONS: number of partitions whose leadership has moved
       ELIGIBLE: number of partitions with leadership eligible to move
   TRANSFERRING: current active number of leadership transfers
         FAILED: number of failed leadership transfers

Notes:

   - When errors are present further information will be available in the logs
     for the corresponding node.

   - Only partitions with more than one replica are eligible for leadership
     transfer.
`,
		Args: cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, _ []string) {
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			config.CheckExitCloudAdmin(p)

			client, err := adminapi.NewClient(cmd.Context(), fs, p)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			brokers, err := client.Brokers(cmd.Context())
			out.MaybeDie(err, "unable to request brokers: %v", err)

			if len(brokers) == 0 {
				out.Die("No brokers found. Check broker address configuration.")
			}

			if brokers[0].Maintenance == nil {
				out.Die("Maintenance mode is not supported in this cluster")
			}

			table := newMaintenanceReportTable()
			defer table.Flush()
			for _, broker := range brokers {
				addBrokerMaintenanceReport(table, broker)
			}
		},
	}
	return cmd
}
