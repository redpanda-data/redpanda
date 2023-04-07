// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package group

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/kafka"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
)

func NewDescribeCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var summary bool

	cmd := &cobra.Command{
		Use:   "describe [GROUPS...]",
		Short: "Describe group offset status & lag",
		Long: `Describe group offset status & lag.

This command describes group members, calculates their lag, and prints detailed
information about the members.
`,
		Args: cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, groups []string) {
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			adm, err := kafka.NewAdmin(fs, p, cfg)
			out.MaybeDie(err, "unable to initialize kafka client: %v", err)
			defer adm.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			described, err := adm.DescribeGroups(ctx, groups...)
			out.HandleShardError("DescribeGroups", err)

			if summary {
				printDescribedSummary(described)
				return
			}

			fetched := adm.FetchManyOffsets(ctx, groups...)
			fetched.EachError(func(r kadm.FetchOffsetsResponse) {
				fmt.Printf("unable to fetch offsets for group %q: %v\n", r.Group, r.Err)
				delete(fetched, r.Group)
			})
			if fetched.AllFailed() {
				out.Die("unable to fetch offsets for any group")
			}

			var listed kadm.ListedOffsets
			listPartitions := described.AssignedPartitions()
			listPartitions.Merge(fetched.CommittedPartitions())
			if topics := listPartitions.Topics(); len(topics) > 0 {
				listed, err = adm.ListEndOffsets(ctx, topics...)
				out.HandleShardError("ListOffsets", err)
			}

			printDescribed(
				described,
				fetched,
				listed,
			)
		},
	}
	cmd.Flags().BoolVarP(&summary, "print-summary", "s", false, "Print only the group summary section")
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
) {
	for _, group := range groups.Sorted() {
		lag := kadm.CalculateGroupLag(group, fetched[group.Group].Fetched, listed)

		var rows []describeRow
		var useInstanceID, useErr bool
		for _, l := range lag.Sorted() {
			row := describeRow{
				topic:     l.End.Topic,
				partition: l.End.Partition,

				currentOffset: strconv.FormatInt(l.Commit.At, 10),
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

			if l.Commit.At == -1 { // nothing committed
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

func printDescribedSummary(groups kadm.DescribedGroups) {
	for _, group := range groups.Sorted() {
		printDescribedGroupSummary(group)
	}
}

func printDescribedGroupSummary(group kadm.DescribedGroup) {
	tw := out.NewTabWriter()
	defer tw.Flush()
	fmt.Fprintf(tw, "GROUP\t%s\n", group.Group)
	fmt.Fprintf(tw, "COORDINATOR\t%d\n", group.Coordinator.NodeID)
	fmt.Fprintf(tw, "STATE\t%s\n", group.State)
	fmt.Fprintf(tw, "BALANCER\t%s\n", group.Protocol)
	fmt.Fprintf(tw, "MEMBERS\t%d\n", len(group.Members))
	if group.Err != nil {
		fmt.Fprintf(tw, "ERROR\t%s\n", group.Err)
	}
}

func printDescribedGroup(
	group kadm.DescribedGroup,
	rows []describeRow,
	useInstanceID bool,
	useErr bool,
) {
	printDescribedGroupSummary(group)

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
			if r.instanceID != nil {
				return append(orig(r), *r.instanceID)
			}
			return append(orig(r), "")
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

	tw := out.NewTable(headers...)
	defer tw.Flush()
	for _, row := range rows {
		tw.Print(args(&row)...)
	}
}
