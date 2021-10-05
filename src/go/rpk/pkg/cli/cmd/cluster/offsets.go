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
	"sort"
	"strconv"

	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/config"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/kafka"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/out"
)

func NewOffsetsCommand(fs afero.Fs) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "offsets",
		Short: "Report cluster offset status",
		Args:  cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, _ []string) {
			p := config.ParamsFromCommand(cmd)
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			cl, err := kafka.NewFranzClient(fs, p, cfg)
			out.MaybeDie(err, "unable to initialize kafka client: %v", err)
			defer cl.Close()

			groups, err := listGroups(cl)
			out.MaybeDieErr(err)

			if len(groups) == 0 {
				out.Exit("There are no groups to describe!")
			}

			described, err := describeGroups(cl, groups)
			out.MaybeDieErr(err)

			fetchedOffsets, err := fetchOffsets(cl, groups)
			out.MaybeDieErr(err)

			listedOffsets, err := listOffsets(cl, described)
			out.MaybeDieErr(err)

			printDescribed(
				described,
				fetchedOffsets,
				listedOffsets,
			)
		},
	}
	return cmd
}

// Command issuing.
//
// Printing the lag for all groups requires four(ish...sharding) requests:
//
//  * list groups
//  * describe groups
//  * fetch offsets
//  * list offsets
//
// List groups, describe groups, and list offsets need to be split and sent to
// all brokers, and fetch offsets needs to be issued once per group. The kgo
// client handles sharding, meaning we can continue in the face of partial
// failures. We only bail on describing if the entire cluster is unreachable
// (basically, we are trying to work around the scenario where one broker is
// down as best as we can).
//
// Request issuing / response handling is pretty rote.

func listGroups(cl *kgo.Client) ([]string, error) {
	req := kmsg.NewPtrListGroupsRequest()

	shards := cl.RequestSharded(context.Background(), req)
	var groups []string
	allFailed := kafka.EachShard(req, shards, func(shard kgo.ResponseShard) {
		resp := shard.Resp.(*kmsg.ListGroupsResponse)
		if err := kerr.ErrorForCode(resp.ErrorCode); err != nil {
			fmt.Printf("ListGroups request to broker %s returned error: %v\n",
				kafka.MetaString(shard.Meta),
				err,
			)
			return
		}
		for _, group := range resp.Groups {
			groups = append(groups, group.Group)
		}
	})
	if allFailed {
		return nil, fmt.Errorf("all %d ListGroups requests failed", len(shards))
	}
	return groups, nil
}

func describeGroups(cl *kgo.Client, groups []string) ([]describedGroup, error) {
	req := kmsg.NewPtrDescribeGroupsRequest()
	req.Groups = groups

	shards := cl.RequestSharded(context.Background(), req)
	var described []describedGroup
	allFailed := kafka.EachShard(req, shards, func(shard kgo.ResponseShard) {
		resp := unmarshalGroupDescribeMembers(shard.Meta, shard.Resp.(*kmsg.DescribeGroupsResponse))
		described = append(described, resp.Groups...)
	})
	if allFailed {
		return nil, fmt.Errorf("all %d DescribeGroups requests failed", len(shards))
	}
	return described, nil
}

type offset struct {
	at  int64
	err error
}

func fetchOffsets(
	cl *kgo.Client, groups []string,
) (map[string]map[int32]offset, error) {
	fetched := make(map[string]map[int32]offset)
	var failures int
	for i := range groups {
		req := kmsg.NewPtrOffsetFetchRequest()
		req.Group = groups[i]
		resp, err := req.RequestWith(context.Background(), cl)
		if err != nil {
			fmt.Printf("Unable to request OffsetFetch: %v\n", err)
			failures++
			continue
		}

		for _, topic := range resp.Topics {
			fetchedt := fetched[topic.Topic]
			if fetchedt == nil {
				fetchedt = make(map[int32]offset)
				fetched[topic.Topic] = fetchedt
			}
			for _, partition := range topic.Partitions {
				fetchedt[partition.Partition] = offset{
					at:  partition.Offset,
					err: kerr.ErrorForCode(partition.ErrorCode),
				}
			}
		}
	}
	if failures == len(groups) {
		return nil, fmt.Errorf("all %d OffsetFetch requests failed", failures)
	}
	return fetched, nil
}

// listOffsets has a tiny bit more logic compared to the others, in that we
// want to list any partition in any topic assigned to any member.
func listOffsets(
	cl *kgo.Client, described []describedGroup,
) (map[string]map[int32]offset, error) {
	tps := make(map[string]map[int32]struct{})
	for _, group := range described {
		for _, member := range group.Members {
			for _, topic := range member.MemberAssignment.Topics {
				if tps[topic.Topic] == nil {
					tps[topic.Topic] = make(map[int32]struct{})
				}
				for _, partition := range topic.Partitions {
					tps[topic.Topic][partition] = struct{}{}
				}
			}
		}
	}

	req := kmsg.NewPtrListOffsetsRequest()
	for topic, partitions := range tps {
		reqTopic := kmsg.NewListOffsetsRequestTopic()
		reqTopic.Topic = topic
		for partition := range partitions {
			reqPartition := kmsg.NewListOffsetsRequestTopicPartition()
			reqPartition.Partition = partition
			reqPartition.Timestamp = -1 // latest
			reqTopic.Partitions = append(reqTopic.Partitions, reqPartition)
		}
		req.Topics = append(req.Topics, reqTopic)
	}

	shards := cl.RequestSharded(context.Background(), req)
	listed := make(map[string]map[int32]offset)
	allFailed := kafka.EachShard(req, shards, func(shard kgo.ResponseShard) {
		resp := shard.Resp.(*kmsg.ListOffsetsResponse)
		for _, topic := range resp.Topics {
			listedt := listed[topic.Topic]
			if listedt == nil {
				listedt = make(map[int32]offset)
				listed[topic.Topic] = listedt
			}
			for _, partition := range topic.Partitions {
				listedt[partition.Partition] = offset{
					at:  partition.Offset,
					err: kerr.ErrorForCode(partition.ErrorCode),
				}
			}
		}
	})
	if allFailed {
		return nil, fmt.Errorf("all %d ListOffsets requests failed", len(shards))
	}
	return listed, nil
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
	groups []describedGroup,
	fetched map[string]map[int32]offset,
	listed map[string]map[int32]offset,
) {
	lookup := func(m map[string]map[int32]offset, topic string, partition int32) offset {
		p := m[topic]
		if p == nil {
			return offset{at: -1}
		}
		o, exists := p[partition]
		if !exists {
			return offset{at: -1}
		}
		return o
	}

	sort.Slice(groups, func(i, j int) bool {
		return groups[i].Group < groups[j].Group
	})

	for _, group := range groups {
		var rows []describeRow
		var useInstanceID, useErr bool
		for _, member := range group.Members {
			for _, topic := range member.MemberAssignment.Topics {
				t := topic.Topic
				for _, p := range topic.Partitions {
					committed := lookup(fetched, t, p)
					end := lookup(listed, t, p)

					row := describeRow{
						topic:     t,
						partition: p,

						logEndOffset: end.at,

						memberID:   member.MemberID,
						instanceID: member.InstanceID,
						clientID:   member.ClientID,
						host:       member.ClientHost,
						err:        committed.err,
					}
					if row.err == nil {
						row.err = end.err
					}

					useErr = row.err != nil
					useInstanceID = row.instanceID != nil

					row.currentOffset = strconv.FormatInt(committed.at, 10)
					if committed.at == -1 {
						row.currentOffset = "-"
					}

					row.lag = strconv.FormatInt(end.at-committed.at, 10)
					if end.at == 0 {
						row.lag = "-"
					} else if committed.at == -1 {
						row.lag = strconv.FormatInt(end.at, 10)
					}

					rows = append(rows, row)

				}
			}
		}

		printDescribedGroup(group, rows, useInstanceID, useErr)
		fmt.Println()
	}
}

func printDescribedGroup(
	group describedGroup, rows []describeRow, useInstanceID bool, useErr bool,
) {
	tw := out.NewTabWriter()
	fmt.Fprintf(tw, "GROUP\t%s\n", group.Group)
	fmt.Fprintf(tw, "COORDINATOR\t%d\n", group.Broker.NodeID)
	fmt.Fprintf(tw, "STATE\t%s\n", group.State)
	fmt.Fprintf(tw, "BALANCER\t%s\n", group.Protocol)
	fmt.Fprintf(tw, "MEMBERS\t%d\n", len(group.Members))
	if err := kerr.ErrorForCode(group.ErrorCode); err != nil {
		fmt.Fprintf(tw, "ERROR\t%s\n", err)
	}
	tw.Flush()

	if len(rows) == 0 {
		return
	}

	sort.Slice(rows, func(i, j int) bool {
		return rows[i].topic < rows[j].topic ||
			rows[i].topic == rows[j].topic &&
				rows[i].partition < rows[j].partition
	})

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

// Below here, we un-binary a DescribeGroupsResponse.
//
// Kafka actually knows nothing about what clients are assigning each other.
// The protocol is agnostic, which makes it extremely extendable (see streams).
// We have to take the blob of bytes and deserialize it. The standard protocol
// is the "consumer" protocol, which deserializes into the kmsg types used
// below. Only streams uses a different protocol (or maybe it is the same
// protocol, but with a super advanced balancer? tough to remember).

type describedGroupMember struct {
	MemberID         string
	InstanceID       *string
	ClientID         string
	ClientHost       string
	MemberMetadata   kmsg.GroupMemberMetadata
	MemberAssignment kmsg.GroupMemberAssignment
}

type describedGroup struct {
	Broker               kgo.BrokerMetadata
	ErrorCode            int16
	Group                string
	State                string
	ProtocolType         string
	Protocol             string
	Members              []describedGroupMember
	AuthorizedOperations int32
}

type describeGroupsResponse struct {
	ThrottleMillis int32
	Groups         []describedGroup
}

func unmarshalGroupDescribeMembers(
	meta kgo.BrokerMetadata, resp *kmsg.DescribeGroupsResponse,
) *describeGroupsResponse {
	dresp := &describeGroupsResponse{
		ThrottleMillis: resp.ThrottleMillis,
	}
	for _, group := range resp.Groups {
		dgroup := describedGroup{
			Broker:               meta,
			ErrorCode:            group.ErrorCode,
			Group:                group.Group,
			State:                group.State,
			ProtocolType:         group.ProtocolType,
			Protocol:             group.Protocol,
			AuthorizedOperations: group.AuthorizedOperations,
		}
		for _, member := range group.Members {
			dmember := describedGroupMember{
				MemberID:   member.MemberID,
				InstanceID: member.InstanceID,
				ClientID:   member.ClientID,
				ClientHost: member.ClientHost,
			}
			dmember.MemberMetadata.ReadFrom(member.ProtocolMetadata)
			dmember.MemberAssignment.ReadFrom(member.MemberAssignment)

			dgroup.Members = append(dgroup.Members, dmember)
		}
		dresp.Groups = append(dresp.Groups, dgroup)
	}

	return dresp
}
