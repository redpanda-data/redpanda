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

func newUnsafeRecoveryCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var (
		dry       bool
		noConfirm bool
		nodes     []int
	)
	cmd := &cobra.Command{
		Use:   "unsafe-recover",
		Short: "Recover unsafely from partitions that have lost majority",
		Long: `Recover unsafely from partitions that have lost majority.

This command allows you to unsafely recover all data adversely affected by the
loss of the nodes specified in the '--from-nodes' flag. This operation is unsafe 
because it allows you to bulk move all partitions that have lost majority when 
nodes are gone and irrecoverable; this may result in data loss.

You can perform a dry run and verify the partitions that will be recovered by 
using the '--dry' flag.
`,
		Args: cobra.NoArgs,
		Run: func(cmd *cobra.Command, _ []string) {
			f := p.Formatter
			if f.Kind != "text" && f.Kind != "help" && !dry {
				out.Die("Format type %q is only valid for dry runs (--dry)", f.Kind)
			}
			if h, ok := f.Help([]rpadmin.MajorityLostPartitions{}); ok {
				out.Exit(h)
			}
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			config.CheckExitCloudAdmin(p)

			cl, err := adminapi.NewClient(cmd.Context(), fs, p)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			plan, err := cl.MajorityLostPartitions(cmd.Context(), nodes)
			out.MaybeDie(err, "unable to get recovery plan: %v", err)
			if len(plan) == 0 {
				out.Exit("There are no partitions to recover from node(s): %v", nodes)
			}
			err = printPlan(f, plan)
			out.MaybeDieErr(err)
			if dry {
				return
			}
			if !noConfirm {
				confirmed, err := out.Confirm("Confirm recovery from these nodes? This may result in data loss")
				out.MaybeDie(err, "unable to confirm recovery plan: %v", err)
				if !confirmed {
					out.Exit("Command execution canceled")
				}
			}
			fmt.Println("Executing recovery plan...")
			err = cl.ForceRecoverFromNode(cmd.Context(), plan, nodes)
			out.MaybeDie(err, "unable to execute recovery plan: %v", err)
			fmt.Println("Successfully queued the recovery plan, you can check the status by running 'rpk cluster partitions balancer-status'")
		},
	}
	p.InstallFormatFlag(cmd)

	cmd.Flags().IntSliceVar(&nodes, "from-nodes", nil, "Comma-separated list of node IDs from which to recover the partitions")
	cmd.Flags().BoolVar(&dry, "dry", false, "Dry run: print the partition movement plan. Does not execute it")
	cmd.Flags().BoolVar(&noConfirm, "no-confirm", false, "Disable confirmation prompt")

	cmd.MarkFlagRequired("from-nodes")
	cmd.MarkFlagsMutuallyExclusive("dry", "no-confirm")

	return cmd
}

func printPlan(f config.OutFormatter, plan []rpadmin.MajorityLostPartitions) error {
	if isText, _, s, err := f.Format(plan); !isText {
		if err != nil {
			return fmt.Errorf("unable to print in the required format %q: %v", f.Kind, err)
		}
		fmt.Println(s)
		return nil
	}
	tw := out.NewTable("namespace", "topic", "partition", "replica-core", "dead-nodes")
	defer tw.Flush()
	for _, p := range plan {
		var replicas []string
		for _, r := range p.Replicas {
			replicas = append(replicas, fmt.Sprintf("%v-%v", r.NodeID, r.Core))
		}
		tw.Print(p.NTP.Ns, p.NTP.Topic, p.NTP.PartitionID, replicas, p.DeadNodes)
	}
	return nil
}
