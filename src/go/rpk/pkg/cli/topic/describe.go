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
		re         bool
		stable     bool
	)
	cmd := &cobra.Command{
		Use:     "describe [TOPICS]",
		Aliases: []string{"info"},
		Short:   "Describe topics",
		Long: `Describe topics.

This command prints detailed information about topics. The output contains
up to three sections: a summary of the topic, the topic configs, and a detailed
partitions section. By default, the summary and configs sections are printed.

The --regex flag (-r) parses arguments as regular expressions
and describes topics that match any of the expressions.

For example,

    describe foo bar            # describe topics foo and bar
    describe -r '^f.*' '.*r$'   # describe any topic starting with f and any topics ending in r
    describe -r '*'             # describe all topics
    describe -r .               # describe any one-character topics

`,

		Args: cobra.MinimumNArgs(1),
		Run: func(_ *cobra.Command, topicArg []string) {
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)

			cl, err := kafka.NewFranzClient(fs, p)
			out.MaybeDie(err, "unable to initialize kafka client: %v", err)
			defer cl.Close()

			adm, err := kafka.NewAdmin(fs, p)
			out.MaybeDie(err, "unable to initialize kafka client: %v", err)
			defer adm.Close()

			if re {
				topicArg, err = regexTopics(adm, topicArg)
				out.MaybeDie(err, "unable to filter topics by regex: %v", err)
			}

			// By default, if neither are specified, we opt in to
			// the config section only.
			if !summary && !configs && !partitions {
				summary, configs = true, true
			}

			// We show all sections if:
			// - "print-all" is used or
			// - more than one topic are specified or matched.
			if all || len(topicArg) > 1 {
				summary, configs, partitions = true, true, true
			} else if len(topicArg) == 0 {
				out.Exit("did not match any topics, exiting.")
			}

			req := kmsg.NewPtrMetadataRequest()
			for _, topic := range topicArg {
				reqTopic := kmsg.NewMetadataRequestTopic()
				reqTopic.Topic = kmsg.StringPtr(topic)
				req.Topics = append(req.Topics, reqTopic)
			}
			resp, err := req.RequestWith(context.Background(), cl)
			out.MaybeDie(err, "unable to request topic metadata: %v", err)

			const (
				secSummary = "summary"
				secConfigs = "configs"
				secPart    = "partitions"
			)

			for i, topic := range resp.Topics {
				sections := out.NewMaybeHeaderSections(
					out.ConditionalSectionHeaders(map[string]bool{
						secSummary: summary,
						secConfigs: configs,
						secPart:    partitions,
					})...,
				)

				sections.Add(secSummary, func() {
					tw := out.NewTabWriter()
					defer tw.Flush()
					tw.PrintColumn("NAME", *topic.Topic)
					if topic.IsInternal {
						tw.PrintColumn("INTERNAL", topic.IsInternal)
					}
					tw.PrintColumn("PARTITIONS", len(topic.Partitions))
					if len(topic.Partitions) > 0 {
						p0 := &topic.Partitions[0]
						tw.PrintColumn("REPLICAS", len(p0.Replicas))
					}
					if err := kerr.ErrorForCode(topic.ErrorCode); err != nil {
						tw.PrintColumn("ERROR", err)
					}
				})

				sections.Add(secConfigs, func() {
					req := kmsg.NewPtrDescribeConfigsRequest()
					reqResource := kmsg.NewDescribeConfigsRequestResource()
					reqResource.ResourceType = kmsg.ConfigResourceTypeTopic
					reqResource.ResourceName = *topic.Topic
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

				sections.Add(secPart, func() {
					offsets := listStartEndOffsets(cl, *topic.Topic, len(topic.Partitions), stable)

					tw := out.NewTable(describePartitionsHeaders(
						topic.Partitions,
						offsets,
					)...)
					defer tw.Flush()
					for _, row := range describePartitionsRows(
						topic.Partitions,
						offsets,
					) {
						tw.Print(row...)
					}
				})

				i++
				if i < len(resp.Topics) {
					fmt.Println()
				}
			}
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
	cmd.Flags().BoolVarP(&re, "regex", "r", false, "Parse arguments as regex; describe any topic that matches any input topic expression")

	cmd.Flags().BoolVar(&stable, "stable", false, "Include the stable offsets column in the partitions section; only relevant if you produce to this topic transactionally")

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
		// The default stableErr is errUnlisted. We avoid listing
		// stable offsets unless the user asks, so by default, we do
		// not print the stable column.
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
func listStartEndOffsets(cl *kgo.Client, topic string, numPartitions int, stable bool) []startStableEndOffset {
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

	// Both HWM and stable offset checks require Timestamp = -1.
	for i := range req.Topics[0].Partitions {
		req.Topics[0].Partitions[i].Timestamp = -1
	}

	// If the user requested stable offsets, we ask for them second. If we
	// requested these before requesting the HWM, then we could show stable
	// being higher than the HWM. Stable offsets are only relevant if
	// transactions are in play.
	if stable {
		req.IsolationLevel = 1
		shards = cl.RequestSharded(context.Background(), req)
		allFailed = kafka.EachShard(req, shards, func(shard kgo.ResponseShard) {
			resp := shard.Resp.(*kmsg.ListOffsetsResponse)
			if len(resp.Topics) > 0 {
				for _, partition := range resp.Topics[0].Partitions {
					o := &offsets[partition.Partition]
					o.stable = partition.Offset
					o.stableErr = kerr.ErrorForCode(partition.ErrorCode)
				}
			}
		})
		if allFailed {
			return offsets
		}
	}

	// Finally, the HWM.
	shards = cl.RequestSharded(context.Background(), req)
	kafka.EachShard(req, shards, func(shard kgo.ResponseShard) {
		resp := shard.Resp.(*kmsg.ListOffsetsResponse)
		if len(resp.Topics) > 0 {
			for _, partition := range resp.Topics[0].Partitions {
				o := &offsets[partition.Partition]
				o.end = partition.Offset
				o.endErr = kerr.ErrorForCode(partition.ErrorCode)
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
