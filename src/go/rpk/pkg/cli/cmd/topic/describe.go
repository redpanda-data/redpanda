// Copyright 2021 Vectorized, Inc.
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

	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/types"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/config"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/kafka"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/out"
)

func NewDescribeCommand(fs afero.Fs) *cobra.Command {
	var (
		all        bool
		summary    bool
		configs    bool
		partitions bool
	)
	cmd := &cobra.Command{
		Use:     "describe [TOPICS...]",
		Aliases: []string{"info"},
		Short:   "Describe a topic.",
		Long: `Describe a topic.

This command prints detailed information about a topic. There are three
potential sections: a summary of the topic, the topic configs, and a detailed
partitions section. By default, the summary and configs sections are printed.
`,

		Args: cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, topicArg []string) {
			p := config.ParamsFromCommand(cmd)
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

			offsets, err := listStartEndOffsets(cl, topic, len(t.Partitions))
			out.MaybeDie(err, "unable to list offsets: %v", err)

			// We optionally include the following columns:
			//  * epoch, if any leader epoch is non-negative
			//  * offline-replicas, if any are offline
			//  * load-error, if metadata indicates load errors any partitions
			//  * last-stable-offset, if it is ever not equal to the high watermark (transactions)

			header("PARTITIONS", partitions, func() {
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

	cmd.Flags().BoolVarP(&summary, "print-summary", "s", false, "print the summary section")
	cmd.Flags().BoolVarP(&configs, "print-configs", "c", false, "print the config section")
	cmd.Flags().BoolVarP(&partitions, "print-partitions", "p", false, "print the detailed partitions section")
	cmd.Flags().BoolVarP(&all, "print-all", "a", false, "print all sections")

	return cmd
}

func getDescribeUsed(
	partitions []kmsg.MetadataResponseTopicPartition,
	offsets []startStableEndOffset,
) (useEpoch, useOffline, useErr, useStable bool) {
	for _, p := range partitions {
		if p.LeaderEpoch != -1 {
			useEpoch = true
		}
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
	epoch, offline, err, stable := getDescribeUsed(partitions, offsets)
	headers := []string{"partition", "leader"}
	if epoch {
		headers = append(headers, "epoch")
	}
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

	epoch, offline, err, stable := getDescribeUsed(partitions, offsets)
	var rows [][]interface{}
	for _, p := range partitions {
		row := []interface{}{p.Partition, p.Leader}
		if epoch {
			row = append(row, p.LeaderEpoch)
		}
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

		// For offsets, if loading the offset had an error, we write
		// the error rather than a number.
		o := offsets[p.Partition]
		if o.startErr == nil {
			row = append(row, o.start)
		} else {
			row = append(row, maybeKerrMessage(o.startErr))
		}
		if stable {
			if o.stableErr == nil {
				row = append(row, o.stable)
			} else {
				row = append(row, maybeKerrMessage(o.stableErr))
			}
		}
		if o.endErr == nil {
			row = append(row, o.end)
		} else {
			row = append(row, maybeKerrMessage(o.endErr))
		}
		rows = append(rows, row)
	}
	return rows
}

// For offset errors, it is either a *kerr.Error or "server error". If a
// *kerr.Error, we want to just print the message ("UNKNOWN_SERVER_ERROR")
// rather than the default Message: Description.
func maybeKerrMessage(err error) interface{} {
	if k := (*kerr.Error)(nil); errors.As(err, &k) {
		return k.Message
	}
	return err
}

type startStableEndOffset struct {
	start     int64
	startErr  error
	stable    int64
	stableErr error
	end       int64
	endErr    error
}

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
) ([]startStableEndOffset, error) {
	offsets := make([]startStableEndOffset, 0, numPartitions)

	for i := 0; i < numPartitions; i++ {
		offsets = append(offsets, startStableEndOffset{
			start:     -1,
			startErr:  errors.New("server error"),
			stable:    -1,
			stableErr: errors.New("server error"),
			end:       -1,
			endErr:    errors.New("server error"),
		})
	}

	// First we ask for the earliest offsets (special timestamp -2).
	req := kmsg.NewPtrListOffsetsRequest()
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
		for _, partition := range resp.Topics[0].Partitions {
			o := &offsets[partition.Partition]
			o.start = partition.Offset
			o.startErr = kerr.ErrorForCode(partition.ErrorCode)
		}
	})
	if allFailed {
		return nil, fmt.Errorf("all %d ListOffsets requests for the log start offsets failed", len(shards))
	}

	// Next we ask for the latest offsets (special timestamp -1).
	for i := range req.Topics[0].Partitions {
		req.Topics[0].Partitions[i].Timestamp = -1
	}
	shards = cl.RequestSharded(context.Background(), req)
	allFailed = kafka.EachShard(req, shards, func(shard kgo.ResponseShard) {
		resp := shard.Resp.(*kmsg.ListOffsetsResponse)
		for _, partition := range resp.Topics[0].Partitions {
			o := &offsets[partition.Partition]
			o.end = partition.Offset
			o.endErr = kerr.ErrorForCode(partition.ErrorCode)
		}
	})
	if allFailed {
		return nil, fmt.Errorf("all %d ListOffsets requests for the high watermark offsets failed", len(shards))
	}

	// Finally, we ask for the last stable offsets (relevant for transactions).
	req.IsolationLevel = 1
	shards = cl.RequestSharded(context.Background(), req)
	allFailed = kafka.EachShard(req, shards, func(shard kgo.ResponseShard) {
		resp := shard.Resp.(*kmsg.ListOffsetsResponse)
		for _, partition := range resp.Topics[0].Partitions {
			o := &offsets[partition.Partition]
			o.stable = partition.Offset
			o.stableErr = kerr.ErrorForCode(partition.ErrorCode)
		}
	})
	if allFailed {
		return nil, fmt.Errorf("all %d ListOffsets requests for the last stable offsets failed", len(shards))
	}

	return offsets, nil
}

type int32s []int32

func (is int32s) sort() []int32 {
	sort.Slice(is, func(i, j int) bool { return is[i] < is[j] })
	if is == nil {
		return []int32{}
	}
	return is
}
