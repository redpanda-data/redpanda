// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// Package group contains group related subcommands.
package group

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/kafka"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
)

func NewCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "group",
		Aliases: []string{"g"},
		Short:   `Describe, list, and delete consumer groups and manage their offsets`,
		Long: `Describe, list, and delete consumer groups and manage their offsets.

Consumer groups allow you to horizontally scale consuming from topics. A
non-group consumer consumes all records from all partitions you assign it. In
contrast, consumer groups allow many consumers to coordinate and divide work.
If you have two members in a group consuming topics A and B, each with three
partitions, then both members consume three partitions. If you add another
member to the group, then each of the three members will consume two
partitions. This allows you to horizontally scale consuming of topics.

The unit of scaling is a single partition. If you add more consumers to a group
than there are are total partitions to consume, then some consumers will be
idle. More commonly, you have many more partitions than consumer group members
and each member consumes a chunk of available partitions. One scenario where
you may want more members than partitions is if you want active standby's to
take over load immediately if any consuming member dies.

How group members divide work is entirely client driven (the "partition
assignment strategy" or "balancer" depending on the client). Brokers know
nothing about how consumers are assigning partitions. A broker's role in group
consuming is to choose which member is the leader of a group, forward that
member's assignment to every other member, and ensure all members are alive
through heartbeats.

Consumers periodically commit their progress when consuming partitions. Through
these commits, you can monitor just how far behind a consumer is from the
latest messages in a partition. This is called "lag". Large lag implies that
the client is having problems, which could be from the server being too slow,
or the client being oversubscribed in the number of partitions it is consuming,
or the server being in a bad state that requires restarting or removing from
the server pool, and so on.

You can manually manage offsets for a group, which allows you to rewind or
forward commits. If you notice that a recent deploy of your consumers had a
bug, you may want to stop all members, rewind the commits to before the latest
deploy, and restart the members with a patch.

This command allows you to list all groups, describe a group (to view the
members and their lag), and manage offsets.
`,
		Args: cobra.ExactArgs(0),
	}
	p.InstallKafkaFlags(cmd)

	cmd.AddCommand(
		newDeleteCommand(fs, p),
		NewDescribeCommand(fs, p),
		newListCommand(fs, p),
		newSeekCommand(fs, p),
		NewOffsetDeleteCommand(fs, p),
	)

	return cmd
}

func newListCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var filterStates []string
	validStates := []string{"PreparingRebalance", "CompletingRebalance", "Stable", "Dead", "Empty"}
	cmd := &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List all groups",
		Long: `List all groups.

This command lists all groups currently known to Redpanda, including empty
groups that have not yet expired. The BROKER column is which broker node is the
coordinator for the group. This command can be used to track down unknown
groups, or to list groups that need to be cleaned up.

The STATE columns shows which state the group is in:
  - PreparingRebalance: The group is preparing to rebalance.
  - CompletingRebalance: The group is waiting on the leader to provide assignments.
  - Stable: The group is not empty and has no group membership changes in process.
  - Dead: Transient state as the group is being removed.
  - Empty: The group currently has no members.
`,
		Args: cobra.ExactArgs(0),
		Run: func(_ *cobra.Command, _ []string) {
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)

			adm, err := kafka.NewAdmin(fs, p)
			out.MaybeDie(err, "unable to initialize kafka client: %v", err)
			defer adm.Close()

			normalizedFilterStates := make([]string, len(filterStates))
			for i, state := range filterStates {
				vsi := slices.IndexFunc(validStates, func(elem string) bool {
					return strings.EqualFold(elem, state)
				})

				if vsi == -1 {
					out.Die("Invalid group state argument: %s", state)
				}

				normalizedFilterStates[i] = validStates[vsi]
			}

			// The broker-side implementation assumes that the state strings are in CamelCase,
			// so use the normalized form in the request
			listed, err := adm.ListGroups(context.Background(), normalizedFilterStates...)
			out.HandleShardError("ListGroups", err)

			groups := listed.Sorted()
			isV4Response := slices.ContainsFunc(groups, func(g kadm.ListedGroup) bool { return g.State != "" })

			// Conditionally hide the STATE column for older brokers that
			// do not return the state of the consumer group
			if !isV4Response {
				tw := out.NewTable("BROKER", "GROUP")
				defer tw.Flush()
				for _, g := range groups {
					tw.PrintStructFields(struct {
						Broker int32
						Group  string
					}{g.Coordinator, g.Group})
				}
			} else {
				tw := out.NewTable("BROKER", "GROUP", "STATE")
				defer tw.Flush()
				for _, g := range groups {
					tw.PrintStructFields(struct {
						Broker int32
						Group  string
						State  string
					}{g.Coordinator, g.Group, g.State})
				}
			}
		},
	}

	allValidStates := strings.Join(validStates, ", ")
	cmd.Flags().StringSliceVarP(&filterStates, "states", "s", []string{}, fmt.Sprintf("Comma-separated list of group states to filter for. Possible states: [%s]", allValidStates))
	cmd.RegisterFlagCompletionFunc("states", func(_ *cobra.Command, _ []string, _ string) ([]string, cobra.ShellCompDirective) {
		return validStates, cobra.ShellCompDirectiveDefault
	})
	return cmd
}

func newDeleteCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	return &cobra.Command{
		Use:   "delete [GROUPS...]",
		Short: "Delete groups from brokers",
		Long: `Delete groups from brokers.

Older versions of the Kafka protocol included a retention_millis field in
offset commit requests. Group commits persisted for this retention and then
eventually expired. Once all commits for a group expired, the group would be
considered deleted.

The retention field was removed because it proved problematic for infrequently
committing consumers: the offsets could be expired for a group that was still
active. If clients use new enough versions of OffsetCommit (versions that have
removed the retention field), brokers expire offsets only when the group is
empty for offset.retention.minutes. Redpanda does not currently support that
configuration (see #2904), meaning offsets for empty groups expire only when
they are explicitly deleted.

You may want to delete groups to clean up offsets sooner than when they
automatically are cleaned up, such as when you create temporary groups for
quick investigation or testing. This command helps you do that.
`,
		Args: cobra.MinimumNArgs(1),
		Run: func(_ *cobra.Command, args []string) {
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)

			adm, err := kafka.NewAdmin(fs, p)
			out.MaybeDie(err, "unable to initialize kafka client: %v", err)
			defer adm.Close()

			deleted, err := adm.DeleteGroups(context.Background(), args...)
			out.HandleShardError("DeleteGroups", err)

			tw := out.NewTable("GROUP", "STATUS")
			defer tw.Flush()
			for _, g := range deleted.Sorted() {
				status := "OK"
				if g.Err != nil {
					status = g.Err.Error()
				}
				tw.PrintStructFields(struct {
					Group  string
					Status string
				}{
					g.Group,
					status,
				})
			}
		},
	}
}
