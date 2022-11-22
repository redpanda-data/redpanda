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

// DescribedGroup contains data from a describe groups response for a single group.
// Mostly copied here from kadm.describedGroup so tags could be added.
// DescribedRows is added since the original code did not group the details and the summary together, but we want them together for structured printing.
type describedGroupForStructuredPrint struct {
	Group string `json:"group" yaml:"group"` // Group is the name of the described group.

	Coordinator   brokerDetail           `json:"coordinator" yaml:"coordinator"`     // Coordinator is the coordinator broker for this group.
	State         string                 `json:"state" yaml:"state"`                 // State is the state this group is in (Empty, Dead, Stable, etc.).
	ProtocolType  string                 `json:"protocol_type" yaml:"protocol_type"` // ProtocolType is the type of protocol the group is using, "consumer" for normal consumers, "connect" for Kafka connect.
	Protocol      string                 `json:"protocol" yaml:"protocol"`           // Protocol is the partition assignor strategy this group is using.
	Members       []describedGroupMember `json:"members" yaml:"members"`             // Members contains the members of this group sorted first by InstanceID, or if nil, by MemberID.
	DescribedRows describedRows          `json:"described_rows" yaml:"described_rows"`

	Err error `json:"err" yaml:"err"` // Err is non-nil if the group could not be described.
}

// Copied from kadm.DescribedGroupMember to add struct tags.
type describedGroupMember struct {
	MemberID   string  `json:"member_id" yaml:"member_id"`     // MemberID is the Kafka assigned member ID of this group member.
	InstanceID *string `json:"instance_id" yaml:"instance_id"` // InstanceID is a potential user assigned instance ID of this group member (KIP-345).
	ClientID   string  `json:"client_id" yaml:"client_id"`     // ClientID is the Kafka client given ClientID of this group member.
	ClientHost string  `json:"client_host" yaml:"client_host"` // ClientHost is the host this member is running on.

	Join     kadm.GroupMemberMetadata   `json:"join" yaml:"join"`         // Join is what this member sent in its join group request; what it wants to consume.
	Assigned kadm.GroupMemberAssignment `json:"assigned" yaml:"assigned"` // Assigned is what this member was assigned to consume by the leader.
}

// Copied from kadm.BrokerDetail to add struct tags.
type brokerDetail struct {
	// NodeID is the broker node ID.
	//
	// Seed brokers will have very negative IDs; kgo does not try to map
	// seed brokers to loaded brokers.
	NodeID int32 `json:"node_id" yaml:"node_id"`

	// Port is the port of the broker.
	Port int32 `json:"port" yaml:"port"`

	// Host is the hostname of the broker.
	Host string `json:"host" yaml:"host"`

	// Rack is an optional rack of the broker. It is invalid to modify this
	// field.
	//
	// Seed brokers will not have a rack.
	Rack *string `json:"rack" yaml:"rack"`

	_ struct{} // allow us to add fields later
}
type describedRows struct {
	Rows          []describeRow `json:"rows" yaml:"rows"`
	UseInstanceID bool          `json:"use_instance_id" yaml:"use_instance_id"`
	UseErr        bool          `json:"use_err" yaml:"use_err"`
}
type describedGroupCollectionForStructuredPrint struct {
	Groups []describedGroupForStructuredPrint `json:"groups" yaml:"groups"`
}

func (collection *describedGroupCollectionForStructuredPrint) addGroup(newGroup describedGroupForStructuredPrint) {
	collection.Groups = append(collection.Groups, newGroup)
}

func NewDescribeCommand(fs afero.Fs) *cobra.Command {
	var (
		summary bool
		format  string
	)

	cmd := &cobra.Command{
		Use:   "describe [GROUPS...]",
		Short: "Describe group offset status & lag",
		Long: `Describe group offset status & lag.

This command describes group members, calculates their lag, and prints detailed
information about the members.
`,
		Args: cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, groups []string) {
			p := config.ParamsFromCommand(cmd)
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			adm, err := kafka.NewAdmin(fs, p, cfg)
			out.MaybeDie(err, "unable to initialize kafka client: %v", err)
			defer adm.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			described, err := adm.DescribeGroups(ctx, groups...)
			out.HandleShardError("DescribeGroups", err)
			groupCollection := describedGroupCollectionForStructuredPrint{}
			for _, describedGroup := range described {
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

				coordinator := brokerDetail{
					NodeID: describedGroup.Coordinator.NodeID,
					Port:   describedGroup.Coordinator.Port,
					Host:   describedGroup.Coordinator.Host,
				}

				members := []describedGroupMember{}
				for _, member := range describedGroup.Members {
					members = append(members, describedGroupMember{
						MemberID:   member.MemberID,
						InstanceID: member.InstanceID,
						ClientID:   member.ClientID,
						ClientHost: member.ClientHost,
						Join:       member.Join,
						Assigned:   member.Assigned,
					})
				}

				groupCollection.addGroup(describedGroupForStructuredPrint{
					Group:         describedGroup.Group,
					Coordinator:   coordinator,
					State:         describedGroup.State,
					Protocol:      describedGroup.Protocol,
					ProtocolType:  describedGroup.ProtocolType,
					Members:       members,
					DescribedRows: getDescribedForGroup(describedGroup, fetched, listed),
				})
			}

			if summary && format == out.FmtText {
				printDescribedSummary(groupCollection)
				return
			}

			if format != out.FmtText {
				out.PrintFormatted(groupCollection, format)
			} else {
				printDescribed(groupCollection)
			}
		},
	}
	cmd.Flags().StringVar(&format, "format", out.FmtText, "Output format (text, json, yaml)")
	cmd.Flags().BoolVarP(&summary, "print-summary", "s", false, "Print only the group summary section")
	return cmd
}

// Below here lies printing the output of everything we have done.
//
// There is not much logic; the main thing to note is that we use dashes when
// some fields do not apply yet, and we only output the instance id or error
// columns if any member in the group has an instance id / error.

type describeRow struct {
	Topic         string  `json:"topic" yaml:"topic"`
	Partition     int32   `json:"partition" yaml:"partition"`
	CurrentOffset string  `json:"current_offset" yaml:"current_offset"`
	LogEndOffset  int64   `json:"log_end_offset" yaml:"log_end_offset"`
	Lag           string  `json:"lag" yaml:"lag"`
	MemberID      string  `json:"member_id" yaml:"member_id"`
	InstanceID    *string `json:"instance_id" yaml:"instance_id"`
	ClientID      string  `json:"client_id" yaml:"client_id"`
	Host          string  `json:"host" yaml:"host"`
	Err           error   `json:"err" yaml:"err"`
}

func printDescribed(
	groups describedGroupCollectionForStructuredPrint,
) {
	for _, group := range groups.Groups {
		printDescribedGroup(group, group.DescribedRows.Rows, group.DescribedRows.UseInstanceID, group.DescribedRows.UseErr)
		fmt.Println()
	}
}

func getDescribedForGroup(
	group kadm.DescribedGroup,
	fetched map[string]kadm.FetchOffsetsResponse,
	listed kadm.ListedOffsets,
) describedRows {
	lag := kadm.CalculateGroupLag(group, fetched[group.Group].Fetched, listed)

	// zero value init so structured prints get [] instead of null
	rows := []describeRow{}
	var useInstanceID, useErr bool
	for _, l := range lag.Sorted() {
		row := describeRow{
			Topic:     l.End.Topic,
			Partition: l.End.Partition,

			CurrentOffset: strconv.FormatInt(l.Commit.At, 10),
			LogEndOffset:  l.End.Offset,
			Lag:           strconv.FormatInt(l.Lag, 10),

			Err: l.Err,
		}

		if !l.IsEmpty() {
			row.MemberID = l.Member.MemberID
			row.InstanceID = l.Member.InstanceID
			row.ClientID = l.Member.ClientID
			row.Host = l.Member.ClientHost
		}

		if l.Commit.At == -1 { // nothing committed
			row.CurrentOffset = "-"
		}
		if l.End.Offset == 0 { // nothing produced yet
			row.Lag = "-"
		}

		useInstanceID = useInstanceID || row.InstanceID != nil
		useErr = useErr || row.Err != nil

		rows = append(rows, row)
	}

	return describedRows{
		Rows:          rows,
		UseInstanceID: useInstanceID,
		UseErr:        useErr,
	}
}

func printDescribedSummary(groups describedGroupCollectionForStructuredPrint) {
	for _, group := range groups.Groups {
		printDescribedGroupSummary(group)
	}
}

func printDescribedGroupSummary(group describedGroupForStructuredPrint) {
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
	group describedGroupForStructuredPrint,
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
			r.Topic,
			r.Partition,
			r.CurrentOffset,
			r.LogEndOffset,
			r.Lag,
			r.MemberID,
		}
	}

	if useInstanceID {
		headers = append(headers, "INSTANCE-ID")
		orig := args
		args = func(r *describeRow) []interface{} {
			if r.InstanceID != nil {
				return append(orig(r), *r.InstanceID)
			}
			return append(orig(r), "")
		}
	}

	{
		headers = append(headers, "CLIENT-ID", "HOST")
		orig := args
		args = func(r *describeRow) []interface{} {
			return append(orig(r), r.ClientID, r.Host)
		}
	}

	if useErr {
		headers = append(headers, "ERROR")
		orig := args
		args = func(r *describeRow) []interface{} {
			return append(orig(r), r.Err)
		}
	}

	tw := out.NewTable(headers...)
	defer tw.Flush()
	for _, row := range rows {
		tw.Print(args(&row)...)
	}
}
