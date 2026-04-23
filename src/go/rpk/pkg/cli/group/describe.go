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

	"github.com/cespare/xxhash"
	jump "github.com/lithammer/go-jump-consistent-hash"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/kafka"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/rpkutil"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
	"go.uber.org/zap"
)

func NewDescribeCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var summary, commitsOnly, lagPerTopic, re, useInstanceID bool
	var partitionCount int
	cmd := &cobra.Command{
		Use:   "describe [GROUPS...]",
		Short: "Describe group offset status & lag",
		Long: `Describe group offset status & lag.

This command describes group members, calculates their lag, and prints detailed
information about the members. COORDINATOR-PARTITION indicates the partition in
the __consumer_offsets topic responsible for the group, if topic details are
available; run with –verbose for more info if it is missing.

The --regex flag (-r) parses arguments as regular expressions
and describes groups that match any of the expressions.
`,
		Example: `
Describe groups foo and bar:
  rpk group describe foo bar

Describe any group starting with f or ending in r:
  rpk group describe '^f.*' '.*r$' --regex

Describe all groups:
  rpk group describe -r '*'

Describe any one-character group:
  rpk group describe -r .
`,
		Args: cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, groups []string) {
			f := p.Formatter
			if h, ok := f.Help(groupDescription{}); ok {
				out.Exit(h)
			}
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)

			adm, err := kafka.NewAdmin(fs, p)
			out.MaybeDie(err, "unable to initialize kafka client: %v", err)
			defer adm.Close()

			if re {
				groups, err = regexGroups(adm, groups)
				out.MaybeDie(err, "unable to filter groups by regex: %v", err)
			}
			if len(groups) == 0 {
				out.Exit("did not match any groups, exiting.")
			}

			metadata, err := adm.Metadata(cmd.Context(), "__consumer_offsets")
			if err != nil {
				zap.L().Sugar().Warnf("unable to identify the number of partitions in __consumer_offsets.")
			} else {
				partitionCount = len(metadata.Topics["__consumer_offsets"].Partitions)
			}

			ctx, cancel := context.WithTimeout(cmd.Context(), p.Defaults().GetCommandTimeout())
			defer cancel()

			lags, err := adm.Lag(ctx, groups...)
			if err != nil {
				out.Die("unable to describe groups: %v", err)
			}

			groupDescriptions := buildDescribed(lags, partitionCount)
			if isText, _, s, err := f.Format(groupDescriptions); !isText {
				out.MaybeDie(err, "unable to format described group: %v", err)
				out.Exit(s)
			}
			if lagPerTopic {
				printLagPerTopic(lags, partitionCount)
				return
			}
			if summary {
				printDescribedSummary(lags, partitionCount)
				return
			}
			printDescribed(commitsOnly, lags, useInstanceID, partitionCount)
		},
	}

	p.InstallFormatFlag(cmd)

	cmd.Flags().BoolVarP(&lagPerTopic, "print-lag-per-topic", "t", false, "Print the aggregated lag per topic")
	cmd.Flags().BoolVarP(&summary, "print-summary", "s", false, "Print only the group summary section")
	cmd.Flags().BoolVarP(&commitsOnly, "print-commits", "c", false, "Print only the group commits section")
	cmd.Flags().BoolVarP(&re, "regex", "r", false, "Parse arguments as regex; describe any group that matches any input group expression")
	cmd.Flags().BoolVarP(&useInstanceID, "instance-ID", "i", false, "Include each group member's instance ID")
	cmd.MarkFlagsMutuallyExclusive("print-summary", "print-commits")
	cmd.MarkFlagsMutuallyExclusive("print-lag-per-topic", "print-commits")
	return cmd
}

// Below here lies printing the output in json/yaml.

// groupDescription provides details about the consumer group.
type groupDescription struct {
	GroupName            string          `json:"group_name" yaml:"group_name"`
	CoordinatorPartition string          `json:"coordinator_partition" yaml:"coordinator_partition"`
	State                string          `json:"state" yaml:"state"`
	Balancer             string          `json:"balancer" yaml:"balancer"`
	Members              int             `json:"members" yaml:"members"`
	CoordinatorNode      int             `json:"coordinator_node" yaml:"coordinator_node"`
	TotalLag             int             `json:"total_lag" yaml:"total_lag"`
	Partitions           []partitionInfo `json:"partitions" yaml:"partitions"`
	MembersDetails       []memberInfo    `json:"members_details" yaml:"members_details"`
}

// partitionInfo is use to provide details about the partition.
type partitionInfo struct {
	Partition      int    `json:"partition" yaml:"partition"`
	CurrentOffset  int    `json:"current_offset" yaml:"current_offset"`
	LogStartOffset int    `json:"log_start_offset" yaml:"log_start_offset"`
	LogEndOffset   int    `json:"log_end_offset" yaml:"log_end_offset"`
	Lag            int    `json:"lag" yaml:"lag"`
	Topic          string `json:"topic" yaml:"topic"`
	MemberID       string `json:"member_id" yaml:"member_id"`
	ClientID       string `json:"client_id" yaml:"client_id"`
	Host           string `json:"host" yaml:"host"`
}

// memberInfo is used to provide details about the members.
type memberInfo struct {
	MemberID        string           `json:"member_id" yaml:"member_id"`
	ClientID        string           `json:"client_id" yaml:"client_id"`
	Host            string           `json:"host" yaml:"host"`
	TopicPartitions []topicPartition `json:"topic_partitions" yaml:"topic_partitions"`
}

// topicPartition will be used for Topic.
type topicPartition struct {
	Topic     string `json:"topic" yaml:"topic"`
	Partition int    `json:"partition" yaml:"partition"`
}

// BuildDescribed gathers all the group information and returns
// it as a groupDescription.
func buildDescribed(lags kadm.DescribedGroupLags, partitionCount int) []groupDescription {
	var groupDescriptions []groupDescription
	var partitionID int32

	for _, group := range lags.Sorted() {
		var groupDescription groupDescription
		groupDescription.GroupName = group.Group
		groupDescription.CoordinatorNode = int(group.Coordinator.NodeID)
		groupDescription.State = group.State
		groupDescription.Balancer = group.Protocol
		groupDescription.Members = len(group.Members)
		groupDescription.TotalLag = int(group.Lag.Total())
		if partitionCount > 0 {
			// We do calculate the partition ID for the group with the following
			// hash-based algorithm, which is Redpanda specific at least at 25.1.x, https://github.com/redpanda-data/redpanda/blob/v25.1.1/src/v/kafka/server/coordinator_ntp_mapper.h#L50-L60
			// Apache Kafka does use a different algorithm, https://github.com/apache/kafka/blob/4.0.0/core/src/main/scala/kafka/coordinator/group/GroupMetadataManager.scala#L192
			hash := xxhash.Sum64String(group.Group)
			partitionID = jump.Hash(hash, int32(partitionCount))
			groupDescription.CoordinatorPartition = fmt.Sprintf("__consumer_offsets/%d", partitionID)
		}

		// Initialize partition info so that it is not null in the JSON output.
		partitionsInfo := []partitionInfo{}
		for _, l := range group.Lag.Sorted() {
			pi := partitionInfo{
				Topic:          l.Topic,
				Partition:      int(l.Partition),
				LogStartOffset: int(l.Start.Offset),
				LogEndOffset:   int(l.End.Offset),
				Lag:            int(l.Lag),
			}
			if l.Commit.At == -1 {
				pi.CurrentOffset = -1
			} else {
				pi.CurrentOffset = int(l.Commit.At)
			}
			if l.Member != nil {
				pi.MemberID = l.Member.MemberID
				pi.ClientID = l.Member.ClientID
				pi.Host = l.Member.ClientHost
			} else {
				pi.MemberID = ""
				pi.ClientID = ""
				pi.Host = ""
			}
			partitionsInfo = append(partitionsInfo, pi)
		}
		groupDescription.Partitions = partitionsInfo
		membersInfo := []memberInfo{}
		for _, m := range group.Members {
			mi := memberInfo{
				MemberID: m.MemberID,
				ClientID: m.ClientID,
				Host:     m.ClientHost,
			}

			// topic partitions for members
			mi.TopicPartitions = []topicPartition{}
			membersInfo = append(membersInfo, mi)
		}
		groupDescription.MembersDetails = membersInfo

		groupDescriptions = append(groupDescriptions, groupDescription)
	}
	return groupDescriptions
}

// Below here lies printing the output of everything we have done.
// There is not much logic; the main thing to note is that we use dashes when
// some fields do not apply yet, and we only output the instance id or error
// columns if any member in the group has an instance id / error.

type describeRow struct {
	topic          string
	partition      int32
	currentOffset  string
	logStartOffset int64
	logEndOffset   int64
	lag            string
	memberID       string
	instanceID     *string
	clientID       string
	host           string
	err            string
}

func printDescribed(commitsOnly bool, lags kadm.DescribedGroupLags, useInstanceID bool, partitionCount int) {
	for i, group := range lags.Sorted() {
		var rows []describeRow
		var useErr bool
		for _, l := range group.Lag.Sorted() {
			row := describeRow{
				topic:     l.Topic,
				partition: l.Partition,

				currentOffset:  strconv.FormatInt(l.Commit.At, 10),
				logStartOffset: l.Start.Offset,
				logEndOffset:   l.End.Offset,
				lag:            strconv.FormatInt(l.Lag, 10),
			}
			if l.Err != nil {
				row.err = l.Err.Error()
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
			useErr = useErr || row.err != ""

			rows = append(rows, row)
		}

		printDescribedGroup(commitsOnly, group, rows, useInstanceID, partitionCount, useErr)
		if i != len(lags)-1 {
			fmt.Println()
		}
	}
}

func printDescribedSummary(groups kadm.DescribedGroupLags, partitionCount int) {
	for i, group := range groups.Sorted() {
		printDescribedGroupSummary(group, partitionCount)
		if i != len(groups)-1 {
			fmt.Println()
		}
	}
}

func printDescribedGroupSummary(group kadm.DescribedGroupLag, partitionCount int) {
	tw := out.NewTabWriter()
	defer tw.Flush()
	fmt.Fprintf(tw, "GROUP\t%s\n", group.Group)
	fmt.Fprintf(tw, "COORDINATOR-NODE\t%d\n", group.Coordinator.NodeID)
	if partitionCount > 0 {
		// We do calculate the partition ID for the group with the following
		// hash-based algorithm, which is Redpanda specific at least at 25.1.x, https://github.com/redpanda-data/redpanda/blob/v25.1.1/src/v/kafka/server/coordinator_ntp_mapper.h#L50-L60
		// Apache Kafka does use a different algorithm, https://github.com/apache/kafka/blob/4.0.0/core/src/main/scala/kafka/coordinator/group/GroupMetadataManager.scala#L192
		hash := xxhash.Sum64String(group.Group)
		partitionID := jump.Hash(hash, int32(partitionCount))
		fmt.Fprintf(tw, "COORDINATOR-PARTITION\t__consumer_offsets/%d\n", partitionID)
	}
	fmt.Fprintf(tw, "STATE\t%s\n", group.State)
	fmt.Fprintf(tw, "BALANCER\t%s\n", group.Protocol)
	fmt.Fprintf(tw, "MEMBERS\t%d\n", len(group.Members))
	fmt.Fprintf(tw, "TOTAL-LAG\t%d\n", group.Lag.Total())
	if group.Error() != nil {
		fmt.Fprintf(tw, "ERROR\t%s\n", group.Error())
	}
}

func printDescribedGroup(
	commitsOnly bool,
	group kadm.DescribedGroupLag,
	rows []describeRow,
	useInstanceID bool,
	partitionCount int,
	useErr bool,
) {
	if !commitsOnly {
		printDescribedGroupSummary(group, partitionCount)
	}
	if len(rows) == 0 {
		return
	}
	if !commitsOnly {
		fmt.Println()
	}

	headers := []string{
		"TOPIC",
		"PARTITION",
		"CURRENT-OFFSET",
		"LOG-START-OFFSET",
		"LOG-END-OFFSET",
		"LAG",
		"MEMBER-ID",
	}
	args := func(r *describeRow) []any {
		return []any{
			r.topic,
			r.partition,
			r.currentOffset,
			r.logStartOffset,
			r.logEndOffset,
			r.lag,
			r.memberID,
		}
	}

	if useInstanceID {
		headers = append(headers, "INSTANCE-ID")
		orig := args
		args = func(r *describeRow) []any {
			if r.instanceID != nil {
				return append(orig(r), *r.instanceID)
			}
			return append(orig(r), "")
		}
	}

	{
		headers = append(headers, "CLIENT-ID", "HOST")
		orig := args
		args = func(r *describeRow) []any {
			return append(orig(r), r.clientID, r.host)
		}
	}

	if useErr {
		headers = append(headers, "ERROR")
		orig := args
		args = func(r *describeRow) []any {
			return append(orig(r), r.err)
		}
	}

	tw := out.NewTable(headers...)
	defer tw.Flush()
	for _, row := range rows {
		tw.Print(args(&row)...)
	}
}

func printLagPerTopic(groups kadm.DescribedGroupLags, partitionCount int) {
	printDescribedSummary(groups, partitionCount)
	fmt.Println()
	tw := out.NewTable("TOPIC", "LAG")
	defer tw.Flush()
	for _, group := range groups.Sorted() {
		for _, topicLag := range group.Lag.TotalByTopic().Sorted() {
			tw.Print(topicLag.Topic, topicLag.Lag)
		}
	}
}

func regexGroups(adm *kadm.Client, expressions []string) ([]string, error) {
	// Now we list all groups to match against our expressions.
	groups, err := adm.ListGroups(context.Background())
	if err != nil {
		return nil, fmt.Errorf("unable to list groups: 	%w", err)
	}

	return rpkutil.RegexListedItems(groups.Groups(), expressions)
}
