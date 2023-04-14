// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package topic

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/kafka"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/types"
)

func newDescribeCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var (
		all        bool
		summary    bool
		configs    bool
		partitions bool
	)
	cmd := &cobra.Command{
		Use:     "describe [TOPIC]",
		Aliases: []string{"info"},
		Short:   "Describe a topic",
		Long: `Describe a topic.

This command prints detailed information about a topic. There are three
potential sections: a summary of the topic, the topic configs, and a detailed
partitions section. By default, the summary and configs sections are printed.
`,

		Args: cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, topicArg []string) {
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			cl, err := kafka.NewFranzClient(fs, p, cfg)
			out.MaybeDie(err, "unable to initialize kafka client: %v", err)
			defer cl.Close()

			topic := topicArg[0]

			// By default, if neither are specified, we opt in to
			// the config section only.
			if !summary && !configs && !partitions {
				summary, configs = true, true
			}
			if all {
				summary, configs, partitions = true, true, true
			}
			var sections int
			for _, b := range []bool{summary, configs, partitions} {
				if b {
					sections++
				}
			}

			header := func(name string, b bool, fn func()) {
				if !b {
					return
				}
				if sections > 1 {
					fmt.Println(name)
					fmt.Println(strings.Repeat("=", len(name)))
					defer fmt.Println()
				}
				fn()
			}

			var t kmsg.MetadataResponseTopic
			{
				req := kmsg.NewPtrMetadataRequest()
				reqTopic := kmsg.NewMetadataRequestTopic()
				reqTopic.Topic = kmsg.StringPtr(topic)
				req.Topics = append(req.Topics, reqTopic)

				resp, err := req.RequestWith(context.Background(), cl)
				out.MaybeDie(err, "unable to request topic metadata: %v", err)
				if len(resp.Topics) != 1 {
					out.Die("metadata response returned %d topics when we asked for 1", len(resp.Topics))
				}
				t = resp.Topics[0]
			}

			header("SUMMARY", summary, func() {
				tw := out.NewTabWriter()
				defer tw.Flush()
				tw.PrintColumn("NAME", *t.Topic)
				if t.IsInternal {
					tw.PrintColumn("INTERNAL", t.IsInternal)
				}
				tw.PrintColumn("PARTITIONS", len(t.Partitions))
				if len(t.Partitions) > 0 {
					p0 := &t.Partitions[0]
					tw.PrintColumn("REPLICAS", len(p0.Replicas))
				}
				if err := kerr.ErrorForCode(t.ErrorCode); err != nil {
					tw.PrintColumn("ERROR", err)
				}
			})

			header("CONFIGS", configs, func() {
				req := kmsg.NewPtrDescribeConfigsRequest()
				reqResource := kmsg.NewDescribeConfigsRequestResource()
				reqResource.ResourceType = kmsg.ConfigResourceTypeTopic
				reqResource.ResourceName = topic
				req.Resources = append(req.Resources, reqResource)

				resp, err := req.RequestWith(context.Background(), cl)
				out.MaybeDie(err, "unable to request configs: %v", err)
				if len(resp.Resources) != 1 {
					out.Die("config response returned %d resources when we asked for 1", len(resp.Resources))
				}
				err = kerr.ErrorForCode(resp.Resources[0].ErrorCode)
				out.MaybeDie(err, "config response contained error: %v", err)

				tw := out.NewTable("KEY", "VALUE", "SOURCE")
				defer tw.Flush()
				types.Sort(resp)
				for _, config := range resp.Resources[0].Configs {
					var val string
					if config.IsSensitive {
						val = "(sensitive)"
					} else if config.Value != nil {
						val = *config.Value
					}
					tw.Print(config.Name, val, config.Source)
				}
			})

			// Everything below here is related to partitions: we
			// list start, stable, and end offsets, and then we
			// format everything.
			if !partitions {
				return
			}

			header("PARTITIONS", partitions, func() {
				offsets := listStartEndOffsets(cl, topic, len(t.Partitions))

				tw := out.NewTable(describePartitionsHeaders(
					t.Partitions,
					offsets,
				)...)
				defer tw.Flush()
				for _, row := range describePartitionsRows(
					t.Partitions,
					offsets,
				) {
					tw.Print(row...)
				}
			})
		},
	}

	cmd.Flags().IntVar(new(int), "page", -1, "deprecated")
	cmd.Flags().IntVar(new(int), "page-size", 20, "deprecated")
	cmd.Flags().BoolVar(new(bool), "watermarks", true, "deprecated")
	cmd.Flags().BoolVar(new(bool), "detailed", false, "deprecated")
	cmd.Flags().MarkDeprecated("page", "deprecated - all partitions are printed if the partition section is requested")
	cmd.Flags().MarkDeprecated("page-size", "deprecated - all partitions are printed if the partition section is requested")
	cmd.Flags().MarkDeprecated("watermarks", "deprecated - watermarks are always printed if the partition section is requested")
	cmd.Flags().MarkDeprecated("detailed", "deprecated - info has been merged into describe, use -p to print detailed information")

	cmd.Flags().BoolVarP(&summary, "print-summary", "s", false, "Print the summary section")
	cmd.Flags().BoolVarP(&configs, "print-configs", "c", false, "Print the config section")
	cmd.Flags().BoolVarP(&partitions, "print-partitions", "p", false, "Print the detailed partitions section")
	cmd.Flags().BoolVarP(&all, "print-all", "a", false, "Print all sections")

	return cmd
}

// We optionally include the following columns:
//   - offline-replicas, if any are offline
//   - load-error, if metadata indicates load errors any partitions
//   - last-stable-offset, if it is ever not equal to the high watermark (transactions)
func getDescribeUsed(partitions []kmsg.MetadataResponseTopicPartition, offsets []startStableEndOffset) (useOffline, useErr, useStable bool) {
	for _, p := range partitions {
		if len(p.OfflineReplicas) > 0 {
			useOffline = true
		}
		if p.ErrorCode != 0 {
			useErr = true
		}
	}
	for _, o := range offsets {
		if o.stableErr == nil && o.endErr == nil && o.stable != o.end {
			useStable = true
		}
	}
	return
}

func describePartitionsHeaders(
	partitions []kmsg.MetadataResponseTopicPartition,
	offsets []startStableEndOffset,
) []string {
	offline, err, stable := getDescribeUsed(partitions, offsets)
	headers := []string{"partition", "leader", "epoch"}
	headers = append(headers, "replicas") // TODO add isr see #1928
	if offline {
		headers = append(headers, "offline-replicas")
	}
	if err {
		headers = append(headers, "load-error")
	}
	headers = append(headers, "log-start-offset")
	if stable {
		headers = append(headers, "last-stable-offset")
	}
	headers = append(headers, "high-watermark")
	return headers
}

func describePartitionsRows(
	partitions []kmsg.MetadataResponseTopicPartition,
	offsets []startStableEndOffset,
) [][]interface{} {
	sort.Slice(partitions, func(i, j int) bool {
		return partitions[i].Partition < partitions[j].Partition
	})

	offline, err, stable := getDescribeUsed(partitions, offsets)
	var rows [][]interface{}
	for _, p := range partitions {
		row := []interface{}{p.Partition, p.Leader, p.LeaderEpoch}
		row = append(row, int32s(p.Replicas).sort())
		if offline {
			row = append(row, int32s(p.OfflineReplicas).sort())
		}
		if err {
			if err := kerr.ErrorForCode(p.ErrorCode); err != nil {
				row = append(row, err)
			} else {
				row = append(row, "-")
			}
		}

		// For offsets, we have three options:
		//   - we listed the offset successfully, we write the number
		//   - list offsets, we write "-"
		//   - the partition had a partition error, we write the kerr.Error message
		o := offsets[p.Partition]
		if o.startErr == nil {
			row = append(row, o.start)
		} else if errors.Is(o.startErr, errUnlisted) {
			row = append(row, "-")
		} else {
			row = append(row, o.startErr.(*kerr.Error).Message) //nolint:errorlint // This error must be kerr.Error, and we want the message
		}
		if stable {
			if o.stableErr == nil {
				row = append(row, o.stable)
			} else if errors.Is(o.stableErr, errUnlisted) {
				row = append(row, "-")
			} else {
				row = append(row, o.stableErr.(*kerr.Error).Message) //nolint:errorlint // This error must be kerr.Error, and we want the message
			}
		}
		if o.endErr == nil {
			row = append(row, o.end)
		} else if errors.Is(o.endErr, errUnlisted) {
			row = append(row, "-")
		} else {
			row = append(row, o.endErr.(*kerr.Error).Message) //nolint:errorlint // This error must be kerr.Error, and we want the message
		}
		rows = append(rows, row)
	}
	return rows
}

type startStableEndOffset struct {
	start     int64
	startErr  error
	stable    int64
	stableErr error
	end       int64
	endErr    error
}

var errUnlisted = errors.New("list failed")

// There are three offsets we are interested in: the log start offsets, the
// last stable offsets, and the high watermarks. Unfortunately this requires
// three requests.
//
// We make some assumptions here that the response will not be buggy: it will
// always contain the one topic we asked for, and it will contain all
// partitions we asked for. The logic below will panic redpanda replies
// incorrectly.
func listStartEndOffsets(
	cl *kgo.Client, topic string, numPartitions int,
) []startStableEndOffset {
	offsets := make([]startStableEndOffset, 0, numPartitions)

	for i := 0; i < numPartitions; i++ {
		offsets = append(offsets, startStableEndOffset{
			start:     -1,
			startErr:  errUnlisted,
			stable:    -1,
			stableErr: errUnlisted,
			end:       -1,
			endErr:    errUnlisted,
		})
	}

	// First we ask for the earliest offsets (special timestamp -2).
	req := kmsg.NewPtrListOffsetsRequest()
	req.ReplicaID = -1
	reqTopic := kmsg.NewListOffsetsRequestTopic()
	reqTopic.Topic = topic
	for i := 0; i < numPartitions; i++ {
		part := kmsg.NewListOffsetsRequestTopicPartition()
		part.Partition = int32(i)
		part.Timestamp = -2 // earliest offset
		reqTopic.Partitions = append(reqTopic.Partitions, part)
	}
	req.Topics = append(req.Topics, reqTopic)
	shards := cl.RequestSharded(context.Background(), req)
	allFailed := kafka.EachShard(req, shards, func(shard kgo.ResponseShard) {
		resp := shard.Resp.(*kmsg.ListOffsetsResponse)
		if len(resp.Topics) > 0 {
			for _, partition := range resp.Topics[0].Partitions {
				o := &offsets[partition.Partition]
				o.start = partition.Offset
				o.startErr = kerr.ErrorForCode(partition.ErrorCode)
			}
		}
	})

	// If we fail entirely on the *first* ListOffsets, we return early and
	// avoid attempting two more times. EachShard prints an error message
	// on shard failures, and we do not want two additional wasted attempts
	// and two additional useless duplicate log messages.
	if allFailed {
		return offsets
	}

	// Next we ask for the latest offsets (special timestamp -1).
	for i := range req.Topics[0].Partitions {
		req.Topics[0].Partitions[i].Timestamp = -1
	}
	shards = cl.RequestSharded(context.Background(), req)
	allFailed = kafka.EachShard(req, shards, func(shard kgo.ResponseShard) {
		resp := shard.Resp.(*kmsg.ListOffsetsResponse)
		if len(resp.Topics) > 0 {
			for _, partition := range resp.Topics[0].Partitions {
				o := &offsets[partition.Partition]
				o.end = partition.Offset
				o.endErr = kerr.ErrorForCode(partition.ErrorCode)
			}
		}
	})
	// It is less likely to succeed on the first attempt and fail the second,
	// but we may as well avoid trying a third if we do.
	if allFailed {
		return offsets
	}

	// Finally, we ask for the last stable offsets (relevant for transactions).
	req.IsolationLevel = 1
	shards = cl.RequestSharded(context.Background(), req)
	kafka.EachShard(req, shards, func(shard kgo.ResponseShard) {
		resp := shard.Resp.(*kmsg.ListOffsetsResponse)
		if len(resp.Topics) > 0 {
			for _, partition := range resp.Topics[0].Partitions {
				o := &offsets[partition.Partition]
				o.stable = partition.Offset
				o.stableErr = kerr.ErrorForCode(partition.ErrorCode)
			}
		}
	})

	return offsets
}

type int32s []int32

func (is int32s) sort() []int32 {
	sort.Slice(is, func(i, j int) bool { return is[i] < is[j] })
	if is == nil {
		return []int32{}
	}
	return is
}
