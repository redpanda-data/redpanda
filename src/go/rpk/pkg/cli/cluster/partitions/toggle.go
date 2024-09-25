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
	"context"
	"fmt"
	"os"

	"github.com/redpanda-data/common-go/rpadmin"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
)

func newPartitionEnableCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var (
		partitions []string
		all        bool
	)
	cmd := &cobra.Command{
		Use:   "enable [TOPIC]",
		Short: "Enable partitions of a topic",
		Long: `Enable partitions of a topic

You may enable all partitions of a topic using the --all flag or you may select 
a set of topic/partitions to enable with the '--partitions/-p' flag.

The partition flag accepts the format {namespace}/{topic}/[partitions...]
where namespace and topic are optional parameters. If the namespace is not
provided, rpk will assume 'kafka'. If the topic is not provided in the flag, rpk
will use the topic provided as an argument to this command.

EXAMPLES

Enable all partitions in topic 'foo'
    rpk cluster partitions enable foo --all

Enable partitions 1,2 and 3 of topic 'bar' in the namespace 'internal'
    rpk cluster partitions enable internal/bar --partitions 1,2,3

Enable partition 1, and 2 of topic 'foo', and partition 5 of topic 'bar' in the 
'internal' namespace'
    rpk cluster partitions enable -p foo/1,2 -p internal/bar/5
`,
		Args: cobra.MaximumNArgs(1),
		Run: func(cmd *cobra.Command, topicArg []string) {
			if !all && partitions == nil {
				fmt.Println("Please select either specific partitions to enable (--partitions) or select all (--all).")
				cmd.Help()
				os.Exit(1)
			}
			if all && len(topicArg) == 0 {
				fmt.Println("You must select a topic.")
				cmd.Help()
				os.Exit(1)
			}
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			config.CheckExitCloudAdmin(p)

			cl, err := adminapi.NewClient(cmd.Context(), fs, p)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			err = runToggle(cmd.Context(), cl, all, topicArg, partitions, "enable")
			out.MaybeDieErr(err)
			if len(partitions) == 0 {
				fmt.Printf("Successfully enabled all partitions in topic %v\n", topicArg[0])
			} else {
				fmt.Printf("Successfully enabled the partitions %v\n", partitions)
			}
		},
	}
	cmd.Flags().StringArrayVarP(&partitions, "partitions", "p", nil, "Comma-separated list of partitions you want to enable. Check help for extended usage")
	cmd.Flags().BoolVarP(&all, "all", "a", false, "If true, enable all partitions for the specified topic")

	cmd.MarkFlagsMutuallyExclusive("partitions", "all")
	return cmd
}

func newPartitionDisableCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var (
		partitions []string
		all        bool
	)
	cmd := &cobra.Command{
		Use:   "disable [TOPIC]",
		Short: "Disable partitions of a topic",
		Long: `Disable partitions of a topic

You may disable all partitions of a topic using the --all flag or you may select 
a set of topic/partitions to disable with the '--partitions/-p' flag.

The partition flag accepts the format {namespace}/{topic}/[partitions...]
where namespace and topic are optional parameters. If the namespace is not
provided, rpk will assume 'kafka'. If the topic is not provided in the flag, rpk
will use the topic provided as an argument to this command.

DISABLED PARTITIONS

Disabling a partition in Redpanda involves prohibiting any data consumption or
production to and from it. All internal processes associated with the partition
are stopped, and it remains unloaded during system startup. This measure aims to
maintain cluster health by preventing issues caused by specific corrupted
partitions that may lead to Redpanda crashes. Although the data remains stored
on disk, Redpanda ceases interaction with the disabled partitions to ensure
system stability.

EXAMPLES

Disable all partitions in topic 'foo'
    rpk cluster partitions disable foo --all

Disable partitions 1,2 and 3 of topic 'bar' in the namespace 'internal'
    rpk cluster partitions disable internal/bar --partitions 1,2,3

Disable partition 1, and 2 of topic 'foo', and partition 5 of topic 'bar' in the 
'internal' namespace' 
    rpk cluster partitions disable -p foo/1,2 -p internal/bar/5
`,
		Args: cobra.MaximumNArgs(1),
		Run: func(cmd *cobra.Command, topicArg []string) {
			if !all && partitions == nil {
				fmt.Println("Please select either specific partitions to disable (--partitions) or select all (--all).")
				cmd.Help()
				os.Exit(1)
			}
			if all && len(topicArg) == 0 {
				fmt.Println("You must select a topic.")
				cmd.Help()
				os.Exit(1)
			}
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			config.CheckExitCloudAdmin(p)

			cl, err := adminapi.NewClient(cmd.Context(), fs, p)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			err = runToggle(cmd.Context(), cl, all, topicArg, partitions, "disable")
			out.MaybeDieErr(err)
			if all {
				fmt.Printf("Successfully disabled all partitions in topic %v\n", topicArg[0])
			} else {
				fmt.Printf("Successfully disabled the partitions %v\n", partitions)
			}
		},
	}
	cmd.Flags().StringArrayVarP(&partitions, "partitions", "p", nil, "Comma-separated list of partitions you want to disable. Use --help for additional information")
	cmd.Flags().BoolVarP(&all, "all", "a", false, "If true, disable all partitions for the specified topic")

	cmd.MarkFlagsMutuallyExclusive("partitions", "all")
	return cmd
}

func runToggle(ctx context.Context, cl *rpadmin.AdminAPI, all bool, topicArg, partitionFlag []string, verb string) error {
	isDisable := verb == "disable"
	if all {
		ns, topicName := nsTopic(topicArg[0])
		err := cl.ToggleAllTopicPartitions(ctx, isDisable, ns, topicName)
		if err != nil {
			return fmt.Errorf("failed to %v all partitions in topic %v/%v: %v", verb, ns, topicName, err)
		}
		return nil
	}
	g, egCtx := errgroup.WithContext(ctx)
	for _, ntp := range partitionFlag {
		ns, topicName, partitions, err := out.ParsePartitionString(ntp)
		if err != nil {
			return err
		}
		if topicName != "" && len(topicArg) > 0 && topicArg[0] != "" {
			return fmt.Errorf("unable to run the command: topic cannot be specified both as an argument and in the --partition flag")
		}
		if topicName == "" {
			if len(topicArg) == 0 {
				return fmt.Errorf("unable to run the command: you must specify the topic either as an argument or in the --partition flag")
			}
			topicName = topicArg[0]
		}
		for _, p := range partitions {
			p := p
			g.Go(func() error {
				err := cl.ToggleTopicPartitions(egCtx, isDisable, ns, topicName, p)
				if err != nil {
					return fmt.Errorf("failed to %v partition %v in topic %v/%v: %v", verb, p, ns, topicName, err)
				}
				return nil
			})
		}
	}
	return g.Wait()
}
