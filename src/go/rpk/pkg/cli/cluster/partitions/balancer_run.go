// Copyright 2024 Redpanda Data, Inc.
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

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newTriggerBalancerCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "balance",
		Short: "Triggers on-demand partition balancing",
		Long: `Triggers on-demand partition balancing.

This command allows you to trigger on-demand partition balancing.

With Redpanda Community Edition, the partition count on each broker
can easily become uneven, which leads to data skewing. To distribute
partitions across brokers, you can run this command to trigger
on-demand partition balancing.

With Redpanda Enterprise Edition, Continuous Data Balancing monitors
broker and rack availability, as well as disk usage, to avoid topic
hotspots. However, there are edge cases where users should manually
trigger partition balancing (such as a node becoming unavailable for
a prolonged time and rejoining the cluster thereafter). In such cases,
you should run this command to trigger partition balancing manually.

After you run this command, monitor the balancer progress using:

    rpk cluster partitions balancer-status

To see more detailed movement status, monitor the progress using:

    rpk cluster partitions move-status
`,

		Args: cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, _ []string) {
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			config.CheckExitCloudAdmin(p)

			cl, err := adminapi.NewClient(cmd.Context(), fs, p)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			err = cl.TriggerBalancer(cmd.Context())
			out.MaybeDie(err, "failed to invoke on-demand partition balancing: %v", err)

			fmt.Println("Successfully triggered on-demand partition balancing.\n\nPlease find the progress with 'rpk cluster partitions balancer-status'.")
		},
	}
	return cmd
}
