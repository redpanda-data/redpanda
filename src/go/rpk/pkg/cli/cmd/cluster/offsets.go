// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
package cluster

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/config"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/kafka"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/out"
)

func NewOffsetsCommand(fs afero.Fs) *cobra.Command {
	var printEmpty bool
	cmd := &cobra.Command{
		Use:   "offsets",
		Short: "Print group offset status & lag.",
		Long: `Print group offset status & lag.

This command describes group members, calculates their lag, and prints detailed
information about the members. By default, if no groups are explicitly listed,
this command describes all groups. Specific groups to describe can be passed as
arguments.

By default, offsets are only printed for non-empty groups. If you are managing
groups manually outside of the context of consumer groups but are using
Redpanda to store offsets, you can use the --print-empty flag to print offsets
for empty groups. If you specify any groups directly, their lag is printed even
if the groups are empty.
`,

		Run: func(cmd *cobra.Command, groups []string) {
			p := config.ParamsFromCommand(cmd)
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			cl, err := kafka.NewFranzClient(fs, p, cfg)
			out.MaybeDie(err, "unable to initialize kafka client: %v", err)
			defer cl.Close()

			adm := kadm.NewClient(cl)

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			printEmpty = printEmpty || len(groups) > 0

			if len(groups) == 0 {
				kgroups, err := adm.ListGroups(ctx)
				out.HandleShardError("ListGroups", err)
				groups = kgroups.Groups()
			}

			if len(groups) == 0 {
				out.Exit("There are no groups to describe!")
			}

			described, err := adm.DescribeGroups(ctx, groups...)
			out.HandleShardError("DescribeGroups", err)

			fetched := adm.FetchManyOffsets(ctx, groups...)
			fetched.EachError(func(r kadm.FetchOffsetsResponse) {
				fmt.Printf("unable to fetch offsets for group %q: %v", r.Group, r.Err)
				delete(fetched, r.Group)
			})
			if fetched.AllFailed() {
				out.Die("unable to fetch offsets for any group")
			}

			listed, err := adm.ListEndOffsets(ctx, described.AssignedPartitions().Topics()...)
			out.HandleShardError("ListOffsets", err)

			printDescribed(
				described,
				fetched,
				listed,
				printEmpty,
			)
		},
	}
	cmd.Flags().BoolVarP(&printEmpty, "print-empty", "e", false, "print lag for empty groups if printing all groups")
	return cmd
}

// Below here lies printing the output of everything we have done.
//
// There is not much logic; the main thing to note is that we use dashes when
// some fields do not apply yet, and we only output the instance id or error
// columns if any member in the group has an instance id / error.

type describeRow struct {
	topic         string
	partition     int32
	currentOffset string
	logEndOffset  int64
	lag           string
	memberID      string
	instanceID    *string
	clientID      string
	host          string
	err           error
}

func printDescribed(
	groups kadm.DescribedGroups,
	fetched map[string]kadm.FetchOffsetsResponse,
	listed kadm.ListedOffsets,
	printEmpty bool,
) {
	for _, group := range groups.Sorted() {
		lag := kadm.CalculateGroupLag(group, fetched[group.Group].Fetched, listed)

		var rows []describeRow
		var useInstanceID, useErr bool
		for _, l := range lag.Sorted() {
			if l.IsEmpty() && !printEmpty {
				continue
			}
			row := describeRow{
				topic:     l.End.Topic,
				partition: l.End.Partition,

				currentOffset: strconv.FormatInt(l.Commit.Offset, 10),
				logEndOffset:  l.End.Offset,
				lag:           strconv.FormatInt(l.Lag, 10),

				err: l.Err,
			}

			if !l.IsEmpty() {
				row.memberID = l.Member.MemberID
				row.instanceID = l.Member.InstanceID
				row.clientID = l.Member.ClientID
				row.host = l.Member.ClientHost
			}

			if l.Commit.Offset == -1 { // nothing committed
				row.currentOffset = "-"
			}
			if l.End.Offset == 0 { // nothing produced yet
				row.lag = "-"
			}

			useInstanceID = useInstanceID || row.instanceID != nil
			useErr = useErr || row.err != nil

			rows = append(rows, row)
		}

		printDescribedGroup(group, rows, useInstanceID, useErr)
		fmt.Println()
	}
}

func printDescribedGroup(
	group kadm.DescribedGroup,
	rows []describeRow,
	useInstanceID bool,
	useErr bool,
) {
	tw := out.NewTabWriter()
	fmt.Fprintf(tw, "GROUP\t%s\n", group.Group)
	fmt.Fprintf(tw, "COORDINATOR\t%d\n", group.Coordinator.NodeID)
	fmt.Fprintf(tw, "STATE\t%s\n", group.State)
	fmt.Fprintf(tw, "BALANCER\t%s\n", group.Protocol)
	fmt.Fprintf(tw, "MEMBERS\t%d\n", len(group.Members))
	if group.Err != nil {
		fmt.Fprintf(tw, "ERROR\t%s\n", group.Err)
	}
	tw.Flush()

	if len(rows) == 0 {
		return
	}

	headers := []string{
		"TOPIC",
		"PARTITION",
		"CURRENT-OFFSET",
		"LOG-END-OFFSET",
		"LAG",
		"MEMBER-ID",
	}
	args := func(r *describeRow) []interface{} {
		return []interface{}{
			r.topic,
			r.partition,
			r.currentOffset,
			r.logEndOffset,
			r.lag,
			r.memberID,
		}
	}

	if useInstanceID {
		headers = append(headers, "INSTANCE-ID")
		orig := args
		args = func(r *describeRow) []interface{} {
			return append(orig(r), r.instanceID)
		}
	}

	{
		headers = append(headers, "CLIENT-ID", "HOST")
		orig := args
		args = func(r *describeRow) []interface{} {
			return append(orig(r), r.clientID, r.host)
		}

	}

	if useErr {
		headers = append(headers, "ERROR")
		orig := args
		args = func(r *describeRow) []interface{} {
			return append(orig(r), r.err)
		}
	}

	tw = out.NewTable(headers...)
	defer tw.Flush()
	for _, row := range rows {
		tw.Print(args(&row)...)
	}
}
