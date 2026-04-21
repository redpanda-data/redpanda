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
	"io"
	"os"
	"slices"
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

Using the --format flag with either JSON or YAML prints all the topic information.

The --regex flag (-r) parses arguments as regular expressions
and describes topics that match any of the expressions.

For example,

    describe foo bar            # describe topics foo and bar
    describe -r '^f.*' '.*r$'   # describe any topic starting with f and any topics ending in r
    describe -r '*'             # describe all topics
    describe -r .               # describe any one-character topics
`,
		Args: cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, topicArg []string) {
			f := p.Formatter
			if h, ok := f.Help([]describedTopic{}); ok {
				out.Exit(h)
			}
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
			// - the formatter is not text (json/yaml).
			if all || len(topicArg) > 1 || !f.IsText() {
				summary, configs, partitions = true, true, true
			} else if len(topicArg) == 0 {
				out.Exit("did not match any topics, exiting.")
			}

			req := kmsg.NewPtrMetadataRequest()
			for _, topic := range topicArg {
				reqTopic := kmsg.NewMetadataRequestTopic()
				reqTopic.Topic = new(topic)
				req.Topics = append(req.Topics, reqTopic)
			}
			resp, err := req.RequestWith(cmd.Context(), cl)
			out.MaybeDie(err, "unable to request topic metadata: %v", err)

			var topicDescriptions []describedTopic
			for _, topic := range resp.Topics {
				var t describedTopic
				if summary {
					t.Summary = buildDescribeTopicSummary(topic)
				}
				if configs {
					cfgResp, cfgErr := prepDescribeTopicConfig(cmd.Context(), topic, cl)
					out.MaybeDieErr(cfgErr)
					err = kerr.ErrorForCode(cfgResp.ErrorCode)
					if err != nil {
						t.cfgErr = err
					}
					t.Configs = buildDescribeTopicConfig(cfgResp.Configs)
				}
				if partitions {
					offsets := listStartEndOffsets(cmd.Context(), cl, *topic.Topic, len(topic.Partitions), stable)
					t.Partitions = buildDescribeTopicPartitions(topic.Partitions, offsets)
				}
				topicDescriptions = append(topicDescriptions, t)
			}

			if err := printDescribedTopics(f, topicDescriptions, os.Stdout); err != nil {
				out.MaybeDie(err, "unable to print topics: %v", err)
			}
		},
	}

	p.InstallFormatFlag(cmd)
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

func printDescribedTopics(f config.OutFormatter, topics []describedTopic, w io.Writer) error {
	for _, t := range topics {
		if t.cfgErr != nil {
			out.MaybeDie(t.cfgErr, "config response contained error: %v", t.cfgErr)
		}
	}
	return out.Render(&f, w, topics)
}

type describedTopic struct {
	Summary    describeTopicSummary     `json:"summary"    yaml:"summary"    header:"SUMMARY,omitempty"`
	Configs    []describeTopicConfig    `json:"configs"    yaml:"configs"    header:"CONFIGS,omitempty"`
	Partitions []describeTopicPartition `json:"partitions" yaml:"partitions" header:"PARTITIONS,omitempty"`
	cfgErr     error
}

type describeTopicSummary struct {
	Name       string `json:"name"       yaml:"name"       table:"NAME"`
	Internal   bool   `json:"internal"   yaml:"internal"   table:"INTERNAL,omitempty"`
	Partitions int    `json:"partitions" yaml:"partitions" table:"PARTITIONS"`
	Replicas   int    `json:"replicas"   yaml:"replicas"   table:"REPLICAS,omitempty"`
	Error      string `json:"error"      yaml:"error"      table:"ERROR,omitempty"`
}

func buildDescribeTopicSummary(topic kmsg.MetadataResponseTopic) describeTopicSummary {
	resp := describeTopicSummary{
		Name:       *topic.Topic,
		Internal:   topic.IsInternal,
		Partitions: len(topic.Partitions),
	}
	if len(topic.Partitions) > 0 {
		resp.Replicas = len(topic.Partitions[0].Replicas)
	}
	if err := kerr.ErrorForCode(topic.ErrorCode); err != nil {
		resp.Error = err.Error()
	}
	return resp
}

type describeTopicConfig struct {
	Key    string `json:"key"    yaml:"key"    table:"KEY"`
	Value  string `json:"value"  yaml:"value"  table:"VALUE"`
	Source string `json:"source" yaml:"source" table:"SOURCE"`
}

func prepDescribeTopicConfig(ctx context.Context, topic kmsg.MetadataResponseTopic, cl *kgo.Client) (*kmsg.DescribeConfigsResponseResource, error) {
	req := kmsg.NewPtrDescribeConfigsRequest()
	reqResource := kmsg.NewDescribeConfigsRequestResource()
	reqResource.ResourceType = kmsg.ConfigResourceTypeTopic
	reqResource.ResourceName = *topic.Topic
	req.Resources = append(req.Resources, reqResource)

	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		return nil, fmt.Errorf("unable to request configs: %v", err)
	}
	if len(resp.Resources) != 1 {
		return nil, fmt.Errorf("config response returned %d resources when we asked for 1", len(resp.Resources))
	}
	return &resp.Resources[0], nil
}

func buildDescribeTopicConfig(configs []kmsg.DescribeConfigsResponseResourceConfig) []describeTopicConfig {
	output := make([]describeTopicConfig, 0, len(configs))
	types.Sort(configs)
	for _, cfg := range configs {
		d := describeTopicConfig{
			Key:    cfg.Name,
			Source: cfg.Source.String(),
		}
		if cfg.IsSensitive {
			d.Value = "(sensitive)"
		} else if cfg.Value != nil {
			d.Value = *cfg.Value
		}
		output = append(output, d)
	}
	return output
}

type describeTopicPartition struct {
	Partition        int32    `json:"partition"                    yaml:"partition"                    table:"PARTITION"`
	Leader           int32    `json:"leader"                       yaml:"leader"                       table:"LEADER"`
	Epoch            int32    `json:"epoch"                        yaml:"epoch"                        table:"EPOCH"`
	Replicas         []int32  `json:"replicas"                     yaml:"replicas"                     table:"REPLICAS"`
	OfflineReplicas  []int32  `json:"offline_replicas,omitempty"   yaml:"offline_replicas,omitempty"   table:"OFFLINE-REPLICAS,wide"`
	LoadError        string   `json:"load_error,omitempty"         yaml:"load_error,omitempty"         table:"LOAD-ERROR,wide"`
	LogStartOffset   int64    `json:"log_start_offset"             yaml:"log_start_offset"             table:"LOG-START-OFFSET"`
	LastStableOffset int64    `json:"last_stable_offset,omitempty" yaml:"last_stable_offset,omitempty" table:"LAST-STABLE-OFFSET,wide"`
	HighWatermark    int64    `json:"high_watermark"               yaml:"high_watermark"               table:"HIGH-WATERMARK"`
	Errors           []string `json:"error,omitempty"              yaml:"error,omitempty"              table:"-"`
}

func buildDescribeTopicPartitions(partitions []kmsg.MetadataResponseTopicPartition, offsets []startStableEndOffset) (resp []describeTopicPartition) {
	sort.Slice(partitions, func(i, j int) bool {
		return partitions[i].Partition < partitions[j].Partition
	})
	for _, p := range partitions {
		row := describeTopicPartition{
			Partition:       p.Partition,
			Leader:          p.Leader,
			Epoch:           p.LeaderEpoch,
			Replicas:        int32s(p.Replicas).sort(),
			OfflineReplicas: int32s(p.OfflineReplicas).sort(),
		}
		if err := kerr.ErrorForCode(p.ErrorCode); err != nil {
			row.LoadError = err.Error()
		}
		o := offsets[p.Partition]
		if o.startErr == nil {
			row.LogStartOffset = o.start
		} else if !errors.Is(o.startErr, errUnlisted) {
			row.LogStartOffset = -1
			err := o.startErr.(*kerr.Error).Message //nolint:errorlint // This error must be kerr.Error, and we want the message
			row.Errors = append(row.Errors, err)
		} else {
			row.LogStartOffset = -1
		}
		if o.stableErr == nil {
			row.LastStableOffset = o.stable
		} else if !errors.Is(o.stableErr, errUnlisted) {
			row.LastStableOffset = -1
			err := o.stableErr.(*kerr.Error).Message //nolint:errorlint // This error must be kerr.Error, and we want the message
			row.Errors = append(row.Errors, err)
		}
		if o.endErr == nil {
			row.HighWatermark = o.end
		} else if !errors.Is(o.endErr, errUnlisted) {
			row.HighWatermark = -1
			err := o.endErr.(*kerr.Error).Message //nolint:errorlint // This error must be kerr.Error, and we want the message
			row.Errors = append(row.Errors, err)
		}
		resp = append(resp, row)
	}
	return resp
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
func listStartEndOffsets(ctx context.Context, cl *kgo.Client, topic string, numPartitions int, stable bool) []startStableEndOffset {
	offsets := make([]startStableEndOffset, 0, numPartitions)

	for range numPartitions {
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
	for i := range numPartitions {
		part := kmsg.NewListOffsetsRequestTopicPartition()
		part.Partition = int32(i)
		part.Timestamp = -2 // earliest offset
		reqTopic.Partitions = append(reqTopic.Partitions, part)
	}
	req.Topics = append(req.Topics, reqTopic)
	shards := cl.RequestSharded(ctx, req)
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
		shards = cl.RequestSharded(ctx, req)
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
	shards = cl.RequestSharded(ctx, req)
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
	slices.Sort(is)
	if is == nil {
		return []int32{}
	}
	return is
}
