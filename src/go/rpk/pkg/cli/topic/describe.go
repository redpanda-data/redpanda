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
	"sort"
	"strconv"

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
				reqTopic.Topic = kmsg.StringPtr(topic)
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
					u := getDescribeUsed(topic.Partitions, offsets)
					t.Partitions = buildDescribeTopicPartitions(topic.Partitions, offsets, u)
					t.u = u
				}
				topicDescriptions = append(topicDescriptions, t)
			}

			if printDescribedTopicsFormatter(f, topicDescriptions, os.Stdout) {
				return
			}
			printDescribedTopics(summary, configs, partitions, topicDescriptions)
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

func printDescribedTopicsFormatter(f config.OutFormatter, topics []describedTopic, w io.Writer) bool {
	if isText, _, t, err := f.Format(topics); !isText {
		out.MaybeDie(err, "unable to print in the requested format %v", err)
		fmt.Fprintln(w, t)
		return true
	}
	return false
}

func printDescribedTopics(summary, configs, partitions bool, topics []describedTopic) {
	const (
		secSummary = "summary"
		secConfigs = "configs"
		secPart    = "partitions"
	)

	for _, topic := range topics {
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
			tw.PrintColumn("NAME", topic.Summary.Name)
			if topic.Summary.Internal {
				tw.PrintColumn("INTERNAL", topic.Summary.Internal)
			}
			tw.PrintColumn("PARTITIONS", topic.Summary.Partitions)
			if topic.Summary.Partitions > 0 {
				tw.PrintColumn("REPLICAS", topic.Summary.Replicas)
			}
			if topic.Summary.Error != "" {
				tw.PrintColumn("ERROR", topic.Summary.Error)
			}
		})
		sections.Add(secConfigs, func() {
			out.MaybeDie(topic.cfgErr, "config response contained error: %v", topic.cfgErr)
			tw := out.NewTable("KEY", "VALUE", "SOURCE")
			defer tw.Flush()
			for _, c := range topic.Configs {
				tw.Print(c.Key, c.Value, c.Source)
			}
		})
		sections.Add(secPart, func() {
			tw := out.NewTable(partitionHeader(topic.u)...)
			defer tw.Flush()
			for _, row := range topic.Partitions {
				tw.PrintStrings(row.Row(topic.u)...)
			}
		})
	}
}

type describedTopic struct {
	Summary    describeTopicSummary     `json:"summary" yaml:"summary"`
	Configs    []describeTopicConfig    `json:"configs" yaml:"configs"`
	Partitions []describeTopicPartition `json:"partitions" yaml:"partitions"`
	u          uses
	cfgErr     error
}

type describeTopicSummary struct {
	Name       string `json:"name" yaml:"name"`
	Internal   bool   `json:"internal" yaml:"internal"`
	Partitions int    `json:"partitions" yaml:"partitions"`
	Replicas   int    `json:"replicas" yaml:"replicas"`
	Error      string `json:"error" yaml:"error"`
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
	Key    string `json:"key" yaml:"key"`
	Value  string `json:"value" yaml:"value"`
	Source string `json:"source" yaml:"source"`
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
	Partition            int32   `json:"partition" yaml:"partition"`
	Leader               int32   `json:"leader" yaml:"leader"`
	Epoch                int32   `json:"epoch" yaml:"epoch"`
	Replicas             []int32 `json:"replicas" yaml:"replicas"`
	OfflineReplicas      []int32 `json:"offline_replicas,omitempty" yaml:"offline_replicas,omitempty"`
	LoadError            string  `json:"load_error,omitempty" yaml:"load_error,omitempty"`
	LogStartOffset       int64   `json:"log_start_offset" yaml:"log_start_offset"`
	logStartOffsetText   any
	LastStableOffset     int64 `json:"last_stable_offset,omitempty" yaml:"last_stable_offset,omitempty"`
	lastStableOffsetText any
	HighWatermark        int64 `json:"high_watermark" yaml:"high_watermark"`
	highWatermarkText    any
	Errors               []string `json:"error,omitempty" yaml:"error,omitempty"`
}

func partitionHeader(u uses) []string {
	headers := []string{
		"partition",
		"leader",
		"epoch",
		"replicas",
	}

	if u.Offline {
		headers = append(headers, "offline-replicas")
	}
	if u.LoadErr {
		headers = append(headers, "load-error")
	}
	headers = append(headers, "log-start-offset")
	if u.Stable {
		headers = append(headers, "last-stable-offset")
	}
	headers = append(headers, "high-watermark")
	return headers
}

type uses struct {
	Offline bool
	LoadErr bool
	Stable  bool
}

func (dp describeTopicPartition) Row(u uses) []string {
	row := []string{
		strconv.FormatInt(int64(dp.Partition), 10),
		strconv.FormatInt(int64(dp.Leader), 10),
		strconv.FormatInt(int64(dp.Epoch), 10),
		fmt.Sprintf("%v", dp.Replicas),
	}

	if u.Offline {
		row = append(row, fmt.Sprintf("%v", dp.OfflineReplicas))
	}

	if u.LoadErr {
		row = append(row, dp.LoadError)
	}
	row = append(row, fmt.Sprintf("%v", dp.logStartOffsetText))

	if u.Stable {
		row = append(row, fmt.Sprintf("%v", dp.lastStableOffsetText))
	}
	row = append(row, fmt.Sprintf("%v", dp.highWatermarkText))
	return row
}

func buildDescribeTopicPartitions(partitions []kmsg.MetadataResponseTopicPartition, offsets []startStableEndOffset, u uses) (resp []describeTopicPartition) {
	sort.Slice(partitions, func(i, j int) bool {
		return partitions[i].Partition < partitions[j].Partition
	})
	for _, p := range partitions {
		row := describeTopicPartition{
			Partition: p.Partition,
			Leader:    p.Leader,
			Epoch:     p.LeaderEpoch,
			Replicas:  int32s(p.Replicas).sort(),
		}
		if u.Offline {
			row.OfflineReplicas = int32s(p.OfflineReplicas).sort()
		}
		if u.LoadErr {
			if err := kerr.ErrorForCode(p.ErrorCode); err != nil {
				row.LoadError = err.Error()
			} else {
				row.LoadError = "-"
			}
		}
		o := offsets[p.Partition]
		if o.startErr == nil {
			row.LogStartOffset = o.start
			row.logStartOffsetText = o.start
		} else if errors.Is(o.startErr, errUnlisted) {
			row.LogStartOffset = -1
			row.logStartOffsetText = "-"
		} else {
			row.LogStartOffset = -1
			err := o.startErr.(*kerr.Error).Message //nolint:errorlint // This error must be kerr.Error, and we want the message
			row.logStartOffsetText = err
			row.Errors = append(row.Errors, err)
		}
		if u.Stable {
			if o.stableErr == nil {
				row.LastStableOffset = o.stable
				row.lastStableOffsetText = o.stable
			} else if errors.Is(o.stableErr, errUnlisted) {
				row.LastStableOffset = -1
				row.lastStableOffsetText = "-"
			} else {
				row.LastStableOffset = -1
				err := o.stableErr.(*kerr.Error).Message //nolint:errorlint // This error must be kerr.Error, and we want the message
				row.lastStableOffsetText = err
				row.Errors = append(row.Errors, err)
			}
		}
		if o.endErr == nil {
			row.HighWatermark = o.end
			row.highWatermarkText = o.end
		} else if errors.Is(o.endErr, errUnlisted) {
			row.HighWatermark = -1
			row.highWatermarkText = "-"
		} else {
			row.HighWatermark = -1
			err := o.endErr.(*kerr.Error).Message //nolint:errorlint // This error must be kerr.Error, and we want the message
			row.highWatermarkText = err
			row.Errors = append(row.Errors, err)
		}
		resp = append(resp, row)
	}
	return resp
}

// We optionally include the following columns:
//   - offline-replicas, if any are offline
//   - load-error, if metadata indicates load errors any partitions
//   - last-stable-offset, if it is ever not equal to the high watermark (transactions)
func getDescribeUsed(partitions []kmsg.MetadataResponseTopicPartition, offsets []startStableEndOffset) (u uses) {
	for _, p := range partitions {
		if len(p.OfflineReplicas) > 0 {
			u.Offline = true
		}
		if p.ErrorCode != 0 {
			u.LoadErr = true
		}
	}
	for _, o := range offsets {
		// The default stableErr is errUnlisted. We avoid listing
		// stable offsets unless the user asks, so by default, we do
		// not print the stable column.
		if o.stableErr == nil && o.endErr == nil && o.stable != o.end {
			u.Stable = true
		}
	}
	return
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
	sort.Slice(is, func(i, j int) bool { return is[i] < is[j] })
	if is == nil {
		return []int32{}
	}
	return is
}
