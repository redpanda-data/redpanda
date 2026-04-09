// Copyright 2025 Redpanda Data, Inc.
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
	"math"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/kafka"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func newAnalyzeCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var (
		re                     bool
		timeout                time.Duration
		minBatchesPerPartition int
		timeRng                string

		all           bool
		pSummary      bool
		pTopics       bool
		pPartitionsBR bool
		pPartitionsBS bool
	)
	cmd := &cobra.Command{
		Use:   "analyze [TOPIC]",
		Short: "Analyze a topic",
		Long:  helpAnalyze,
		Args:  cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, topics []string) {
			ctx, cancel := context.WithTimeout(cmd.Context(), timeout)
			defer cancel()

			f := p.Formatter
			if h, ok := f.Help([]rawResults{}); ok {
				out.Exit(h)
			}

			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)

			adm, err := kafka.NewAdmin(fs, p)
			out.MaybeDie(err, "failed to initialize admin kafka client: %v", err)
			defer adm.Close()

			if re {
				topics, err = regexTopics(adm, topics)
				out.MaybeDie(err, "failed to filter topics by regex: %v", err)
				if len(topics) == 0 {
					out.Die("no topics were found that match the given regex")
				}
			}

			err = validateTopics(ctx, adm, topics)
			out.MaybeDie(err, "failed to validate topics: %v", err)

			tr, err := parseTimeRange(timeRng, time.Now())
			out.MaybeDie(err, "failed to parse offset: %v", err)

			offsets, err := getOffsetsForTimeRange(ctx, adm, tr, topics)
			out.MaybeDie(err, "failed to get topic offsets: %v", err)

			var topicPartitions []topicPartition
			offsets.startOffsets.Each(func(o kadm.Offset) {
				topicPartitions = append(topicPartitions, topicPartition{o.Topic, o.Partition})
			})

			c, err := newCollector(collectorConfig{
				topicPartitions: topicPartitions,
				consumeRanges:   offsets,
				timeRange:       tr,
			})
			out.MaybeDie(err, "failed to create collector: %v", err)

			clientCreationFn := func(opts []kgo.Opt) (*kgo.Client, error) {
				return kafka.NewFranzClient(fs, p, opts...)
			}

			err = c.collect(ctx, clientCreationFn)
			out.MaybeDie(err, "failed to collect from topic(s): %v", err)
			res := c.results()

			printed, err := res.printRawResults(f, os.Stdout)
			out.MaybeDie(err, "failed to print results: %v", err)
			if !printed {
				anySet := all || pSummary || pTopics || pPartitionsBR || pPartitionsBS
				printCfg := printConfig{
					percentiles:                    []float64{25, 50, 75, 99},
					printGlobalSummary:             !anySet || all || pSummary,
					printTopicSummary:              !anySet || all || pTopics,
					printPartitionBatchRateSection: all || pPartitionsBR,
					printPartitionBatchSizeSection: all || pPartitionsBS,
				}
				err = res.printSummaries(printCfg)
				out.MaybeDie(err, "failed to print results: %v", err)
			}
		},
	}

	p.InstallFormatFlag(cmd)
	cmd.Flags().BoolVarP(&re, "regex", "r", false, "Parse arguments as regex; analyze any topic that matches any input topic expression")
	cmd.Flags().DurationVar(&timeout, "timeout", 10*time.Second, "Specifies how long the command should run before timing out")
	cmd.Flags().IntVar(&minBatchesPerPartition, "batches", 10, "Minimum number of batches to consume per partition")
	cmd.Flags().StringVarP(&timeRng, "time-range", "t", "-1m:end", "Time range to consume from (-24h:end, -48h:-24h, 2022-02-14:1h)")

	cmd.Flags().BoolVarP(&all, "print-all", "a", false, "Print all sections")
	cmd.Flags().BoolVarP(&pSummary, "print-summary", "s", false, "Print the summary section")
	cmd.Flags().BoolVar(&pTopics, "print-topics", false, "Print the topics section")
	cmd.Flags().BoolVar(&pPartitionsBR, "print-partition-batch-rate", false, "Print the detailed partitions batch rate section")
	cmd.Flags().BoolVar(&pPartitionsBS, "print-partition-batch-size", false, "Print the detailed partitions batch size section")

	return cmd
}

func validateTopics(ctx context.Context, adm *kadm.Client, topics []string) error {
	listed, err := adm.ListTopics(ctx, topics...)
	if err != nil {
		return err
	}

	for _, td := range listed {
		if td.Err != nil {
			return fmt.Errorf("unable to access topic %q: %v", td.Topic, td.Err.Error())
		}
	}

	return nil
}

type offsetRanges struct {
	startOffsets kadm.Offsets
	endOffsets   kadm.Offsets
}

// For each topic/partition if there is offsets in `tr` then the offsets (so, eo)
// will be returned where `so` is after `tr.Start` and `eo` is after `tr.End`.
func getOffsetsForTimeRange(ctx context.Context, adm *kadm.Client, tr timeRange, topics []string) (offsetRanges, error) {
	var offsets offsetRanges

	lstart, err := adm.ListOffsetsAfterMilli(ctx, tr.Start.UnixMilli(), topics...)
	if err != nil {
		return offsets, err
	}
	if lstart.Error() != nil {
		return offsets, fmt.Errorf("unable to list start offsets: %v", lstart.Error())
	}
	offsets.startOffsets = lstart.Offsets()

	lend, err := adm.ListOffsetsAfterMilli(ctx, tr.End.UnixMilli(), topics...)
	if err != nil {
		return offsets, err
	}
	if lend.Error() != nil {
		return offsets, fmt.Errorf("unable to list end offsets: %v", lend.Error())
	}
	offsets.endOffsets = lend.Offsets()

	// Ensure every start offset has an end offset and vice-versa.
	offsets.startOffsets.KeepFunc(func(so kadm.Offset) bool {
		_, exists := offsets.endOffsets.Lookup(so.Topic, so.Partition)
		return exists
	})
	offsets.endOffsets.KeepFunc(func(so kadm.Offset) bool {
		_, exists := offsets.startOffsets.Lookup(so.Topic, so.Partition)
		return exists
	})

	return offsets, nil
}

// Used to allow for time.Time to be printed correctly in '--format help'.
type Time struct {
	time.Time
}

// MarshalText implements encoding.TextMarshaler.
func (t Time) MarshalText() ([]byte, error) {
	return t.Time.MarshalText()
}

// UnmarshalText implements encoding.TextUnmarshaler.
func (t *Time) UnmarshalText(text []byte) error {
	return t.Time.UnmarshalText(text)
}

func (*Time) YamlTypeNameForTest() string { return "timestamp" }

type timeRange struct {
	Start Time `json:"start" yaml:"start"`
	End   Time `json:"end" yaml:"end"`
}

func parseTimeRange(rangeStr string, currentTimestamp time.Time) (tr timeRange, err error) {
	length, startAt, end, fromTimestamp, err := parseTimestampBasedOffset(rangeStr, currentTimestamp)
	tr.Start = Time{startAt}
	if err != nil {
		return
	} else if end {
		err = errors.New("'end' is not a valid value for t1 in 't1:t2'")
		return
	} else if length == len(rangeStr) {
		err = errors.New("timerange must be of form 't1:t2'")
		return
	}

	relativeTimestamp := Time{currentTimestamp}
	if fromTimestamp {
		relativeTimestamp = tr.Start
	}

	if rangeStr[length] != ':' {
		err = errors.New("timerange must be of form 't1:t2'")
		return
	}

	rangeStr = rangeStr[length+1:]
	_, trEnd, end, _, err := parseTimestampBasedOffset(rangeStr, relativeTimestamp.Time)
	tr.End = Time{trEnd}

	if end {
		tr.End = Time{currentTimestamp}
	}

	if !tr.End.After(tr.Start.Time) {
		err = errors.New("t1 has to be less than t2 in timerange t1:t2")
	}

	return
}

type topicPartition struct {
	Topic     string `json:"topic" yaml:"topic"`
	Partition int32  `json:"partition" yaml:"partition"`
}

type topicPartitionInfo struct {
	mux sync.Mutex // grab for all upates to topicPartitionInfo's fields

	StartTS        Time  `json:"start_timestamp" yaml:"start_timestamp"`
	EndTS          Time  `json:"end_timestamp" yaml:"end_timestamp"`
	BatchesRead    int   `json:"batches_read" yaml:"batches_read"`
	BatchSizes     []int `json:"batch_sizes" yaml:"batch_sizes"`
	RecordsRead    int   `json:"records_read" yaml:"records_read"`
	LastOffsetRead int64 `json:"last_offset_read" yaml:"last_offset_read"`
	// Any members below are set at intiialization and constant thereafter.
	TargetStartOffset int64 `json:"target_start_offset" yaml:"target_start_offset"`
	TargetEndOffset   int64 `json:"target_end_offset" yaml:"target_end_offset"`
}

// Returns true if no samples were taken from the topic/partition.
func (tp *topicPartitionInfo) empty() bool {
	return tp.BatchesRead == 0
}

type tpInfoMap map[topicPartition]*topicPartitionInfo

type collectorConfig struct {
	topicPartitions []topicPartition
	consumeRanges   offsetRanges
	timeRange       timeRange
}

type collector struct {
	cfg collectorConfig

	tpInfoMux sync.Mutex // grab for any r/w's to tpInfo
	tpInfo    tpInfoMap

	tpsRemaining map[topicPartition]struct{}
}

func newCollector(cfg collectorConfig) (*collector, error) {
	tpInfo := make(map[topicPartition]*topicPartitionInfo)

	for _, tp := range cfg.topicPartitions {
		so, _ := cfg.consumeRanges.startOffsets.Lookup(tp.Topic, tp.Partition)
		eo, _ := cfg.consumeRanges.endOffsets.Lookup(tp.Topic, tp.Partition)
		tpInfo[tp] = &topicPartitionInfo{
			TargetStartOffset: so.At,
			TargetEndOffset:   eo.At,
		}
	}

	// Only collect from topic/partitions if the starting offset is less than the end.
	cfg.consumeRanges.startOffsets.KeepFunc(func(so kadm.Offset) bool {
		eo, _ := cfg.consumeRanges.endOffsets.Lookup(so.Topic, so.Partition)
		return so.At < eo.At
	})

	c := &collector{
		cfg:          cfg,
		tpInfo:       tpInfo,
		tpsRemaining: make(map[topicPartition]struct{}),
	}

	return c, nil
}

func (c *collector) getClientOps() ([]kgo.Opt, error) {
	opts := []kgo.Opt{
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	}

	startOffsets := make(map[string]map[int32]kgo.Offset)
	c.cfg.consumeRanges.startOffsets.Each(func(so kadm.Offset) {
		o := kgo.NewOffset().At(so.At)
		if _, exists := startOffsets[so.Topic]; !exists {
			startOffsets[so.Topic] = make(map[int32]kgo.Offset)
		}
		startOffsets[so.Topic][so.Partition] = o
	})

	opts = append(opts, kgo.ConsumePartitions(startOffsets))
	opts = append(opts, kgo.WithHooks(c))

	return opts, nil
}

func (c *collector) getTPInfo(topic string, partition int32) *topicPartitionInfo {
	c.tpInfoMux.Lock()
	defer c.tpInfoMux.Unlock()

	tp := topicPartition{topic, partition}
	return c.tpInfo[tp]
}

func (c *collector) OnFetchRecordBuffered(r *kgo.Record) {
	info := c.getTPInfo(r.Topic, r.Partition)
	info.mux.Lock()
	defer info.mux.Unlock()

	if info.StartTS.IsZero() {
		info.StartTS = Time{r.Timestamp}
	}

	if info.EndTS.Before(r.Timestamp) {
		info.EndTS = Time{r.Timestamp}
	}

	info.RecordsRead += 1
	info.LastOffsetRead = r.Offset
}

func (c *collector) OnFetchBatchRead(_ kgo.BrokerMetadata, topic string, partition int32, metrics kgo.FetchBatchMetrics) {
	info := c.getTPInfo(topic, partition)
	info.mux.Lock()
	defer info.mux.Unlock()

	info.BatchesRead += 1
	info.BatchSizes = append(info.BatchSizes, metrics.CompressedBytes)
}

var ( // Ensure collector correctly implements the hook interfaces.
	_ kgo.HookFetchBatchRead      = new(collector)
	_ kgo.HookFetchRecordBuffered = new(collector)
)

func (c *collector) checkForCompletedTPs() map[string][]int32 {
	ret := make(map[string][]int32)

	for tp := range c.tpsRemaining {
		eo, _ := c.cfg.consumeRanges.endOffsets.Lookup(tp.Topic, tp.Partition)
		info := c.getTPInfo(tp.Topic, tp.Partition)
		info.mux.Lock()
		completed := info.LastOffsetRead >= (eo.At - 1)
		info.mux.Unlock()

		if completed {
			ret[tp.Topic] = append(ret[tp.Topic], tp.Partition)
			delete(c.tpsRemaining, tp)
		}
	}

	return ret
}

func (c *collector) isDone() bool {
	return len(c.tpsRemaining) == 0
}

func (c *collector) collect(ctx context.Context, clCreationFn func([]kgo.Opt) (*kgo.Client, error)) error {
	if len(c.cfg.consumeRanges.startOffsets) == 0 {
		// Nothing to collect.
		return nil
	}

	opts, err := c.getClientOps()
	if err != nil {
		return err
	}

	// Creating and closing the kafka client in this method is important as the goroutines it
	// creates concurrently access `a.tpInfo`.
	cl, err := clCreationFn(opts)
	if err != nil {
		return err
	}
	defer cl.Close()

	for _, tp := range c.cfg.topicPartitions {
		_, exists := c.cfg.consumeRanges.startOffsets.Lookup(tp.Topic, tp.Partition)
		if exists {
			c.tpsRemaining[tp] = struct{}{}
		}
	}

	for {
		fetches := cl.PollFetches(ctx)
		if err := fetches.Err(); err != nil {
			return err
		}

		// Note that the fetched records are not being read here. Instead two client hooks are registered
		// "HookFetchBatchRead" and "HookFetchRecordBuffered".
		//
		// The "HookFetchBatchRead" is the only way to get the number of batches read and the compressed
		// batch size. This hook, however, is called for every internally read batch regardless of whether
		// its records end up being returned from "PollFetches" or not.
		//
		// Hence "HookFetchRecordBuffered" is used to ensure that all records have been read for every batch
		// "HookFetchBatchRead" is called for.

		completedTPs := c.checkForCompletedTPs()
		cl.PauseFetchPartitions(completedTPs)

		if c.isDone() {
			return nil
		}
	}
}

type collectResult struct {
	TopicPartitionInfo tpInfoMap `json:"topic_partition_info" yaml:"topic_partition_info"`
	TimeRange          timeRange `json:"time_range" yaml:"time_range"`
}

func (c *collector) results() collectResult {
	return collectResult{c.tpInfo, c.cfg.timeRange}
}

type number interface {
	int | float64
}

// Uses the nearest-rank method to calculate the value of a given percentile.
// Expects `sortedData` to be sorted in ascending order.
// Expects `percent` to be a value in [0, 100].
func percentile[V number](sortedData []V, percent float64) (V, error) {
	if len(sortedData) == 0 {
		return 0, fmt.Errorf("empty input")
	}
	if percent > 100 || percent < 0 {
		return 0, fmt.Errorf("percent must be a value in [0, 100]")
	}

	i := int64(math.Ceil((percent/100.0)*float64(len(sortedData))) - 1)
	i = max(i, 0)

	return sortedData[i], nil
}

type percentileValue[V any] struct {
	percentile float64
	value      V
}

type partitionSummary struct {
	topic            string
	partitions       int
	batchesPerSecond []percentileValue[float64]
	batchSizes       []percentileValue[int] // bytes
}

func (cr *collectResult) summarizePartitions(percentiles []float64) ([]partitionSummary, error) {
	totalDuration := cr.TimeRange.End.Sub(cr.TimeRange.Start.Time)
	tps := map[string]struct {
		partitions int
		batchSizes []int
		batchRates []float64
	}{}
	for tp, info := range cr.TopicPartitionInfo {
		e := tps[tp.Topic]
		e.partitions += 1

		if info.empty() {
			tps[tp.Topic] = e
			continue
		}

		batchesPerS := float64(info.BatchesRead) / totalDuration.Seconds()
		e.batchRates = append(e.batchRates, batchesPerS)

		if !slices.IsSorted(info.BatchSizes) {
			slices.Sort(info.BatchSizes)
		}

		p50, err := percentile(info.BatchSizes, 50.0)
		if err != nil {
			return nil, err
		}
		// Note that we insert the p50 batch size for each partition here.
		// Another option could be inserting every sampled batch size then
		// take percentiles of that. However, since each partition may have
		// a different number of sampled batches care would need to be taken
		// to insert the same number of samples per partition to avoid bias.
		e.batchSizes = append(e.batchSizes, p50)

		tps[tp.Topic] = e
	}

	var pss []partitionSummary
	for t, i := range tps {
		ps := partitionSummary{topic: t, partitions: i.partitions}

		slices.Sort(i.batchSizes)
		slices.Sort(i.batchRates)
		for _, p := range percentiles {
			if len(i.batchSizes) != 0 {
				pbs, err := percentile(i.batchSizes, p)
				if err != nil {
					return nil, err
				}
				ps.batchSizes = append(ps.batchSizes, percentileValue[int]{p, pbs})
			} else {
				ps.batchSizes = append(ps.batchSizes, percentileValue[int]{p, 0})
			}

			if len(i.batchRates) != 0 {
				pbr, err := percentile(i.batchRates, p)
				if err != nil {
					return nil, err
				}
				ps.batchesPerSecond = append(ps.batchesPerSecond, percentileValue[float64]{p, pbr})
			} else {
				ps.batchesPerSecond = append(ps.batchesPerSecond, percentileValue[float64]{p, 0})
			}
		}

		pss = append(pss, ps)
	}

	return pss, nil
}

type topicSummary struct {
	topic                 string
	partitions            int
	totalBatchesPerSecond float64
	averageBatchSize      float64 // bytes
	throughput            float64 // bytes/second
}

func (cr *collectResult) summarizeTopics() ([]topicSummary, error) {
	tsm := map[string]struct {
		partitions   int
		totalBatches int
		totalBytes   int
	}{}

	for tp, info := range cr.TopicPartitionInfo {
		ts := tsm[tp.Topic]
		ts.partitions += 1
		ts.totalBatches += info.BatchesRead
		for _, bs := range info.BatchSizes {
			ts.totalBytes += bs
		}
		tsm[tp.Topic] = ts
	}

	totalDuration := cr.TimeRange.End.Sub(cr.TimeRange.Start.Time)

	var tss []topicSummary
	for t, ts := range tsm {
		averageBatchSize := 0.0
		if ts.totalBatches > 0 {
			averageBatchSize = float64(ts.totalBytes) / float64(ts.totalBatches)
		}
		tss = append(tss, topicSummary{
			topic:                 t,
			partitions:            ts.partitions,
			totalBatchesPerSecond: float64(ts.totalBatches) / totalDuration.Seconds(),
			averageBatchSize:      averageBatchSize,
			throughput:            float64(ts.totalBytes) / totalDuration.Seconds(),
		})
	}

	return tss, nil
}

type globalSummary struct {
	topics                int
	partitions            int
	totalBatchesPerSecond float64
	averageBatchSize      float64 // bytes
	throughput            float64 // bytes/second
}

func (cr *collectResult) summarizeGlobal() (globalSummary, error) {
	parts := 0
	totalBatches := 0
	totalDuration := cr.TimeRange.End.Sub(cr.TimeRange.Start.Time)
	totalBytes := 0
	topics := map[string]struct{}{}

	for tp, info := range cr.TopicPartitionInfo {
		parts += 1
		topics[tp.Topic] = struct{}{}

		if info.empty() {
			continue
		}

		totalBatches += info.BatchesRead

		for _, bs := range info.BatchSizes {
			totalBytes += bs
		}
	}

	averageBatchSize := 0.0
	if totalBatches > 0 {
		averageBatchSize = float64(totalBytes) / float64(totalBatches)
	}

	return globalSummary{
		topics:                len(topics),
		partitions:            parts,
		totalBatchesPerSecond: float64(totalBatches) / totalDuration.Seconds(),
		throughput:            float64(totalBytes) / totalDuration.Seconds(),
		averageBatchSize:      averageBatchSize,
	}, nil
}

type printConfig struct {
	percentiles                    []float64
	printGlobalSummary             bool
	printTopicSummary              bool
	printPartitionBatchRateSection bool
	printPartitionBatchSizeSection bool
}

func (cr *collectResult) printSummaries(cfg printConfig) error {
	var err error

	percentiles := cfg.percentiles
	percentileNames := []string{}
	for _, p := range percentiles {
		percentileNames = append(percentileNames, fmt.Sprintf("P%v", p))
	}

	const (
		secSummary      = "summary"
		secTopicSummary = "topic summary"
		secBatchRate    = "partition batch rate (batches/s)"
		secBatchSize    = "partition batch size (bytes)"
	)

	var gs globalSummary
	if cfg.printGlobalSummary {
		gs, err = cr.summarizeGlobal()
		if err != nil {
			return err
		}
	}

	var tss []topicSummary
	if cfg.printTopicSummary {
		tss, err = cr.summarizeTopics()
		if err != nil {
			return err
		}
		// Print the topic summaries in asc order by topic name
		slices.SortFunc(tss, func(a topicSummary, b topicSummary) int {
			return strings.Compare(a.topic, b.topic)
		})
	}

	var pss []partitionSummary
	if cfg.printPartitionBatchRateSection || cfg.printPartitionBatchSizeSection {
		pss, err = cr.summarizePartitions(percentiles)
		if err != nil {
			return err
		}
		// Print the partition summaries in asc order by topic name
		slices.SortFunc(pss, func(a partitionSummary, b partitionSummary) int {
			return strings.Compare(a.topic, b.topic)
		})
	}

	sections := out.NewMaybeHeaderSections(
		out.ConditionalSectionHeaders(map[string]bool{
			secSummary:      cfg.printGlobalSummary,
			secTopicSummary: cfg.printTopicSummary,
			secBatchRate:    cfg.printPartitionBatchRateSection,
			secBatchSize:    cfg.printPartitionBatchSizeSection,
		})...,
	)

	sections.Add(secSummary, func() {
		tw := out.NewTabWriter()
		defer tw.Flush()
		tw.PrintColumn("topics", gs.topics)
		tw.PrintColumn("partitions", gs.partitions)
		tw.PrintColumn("total throughput (bytes/s)", gs.throughput)
		tw.PrintColumn("total batch rate (batches/s)", gs.totalBatchesPerSecond)
		tw.PrintColumn("average batch size (bytes)", gs.averageBatchSize)
	})

	sections.Add(secTopicSummary, func() {
		tw := out.NewTable("TOPIC", "PARTITIONS", "BYTES-PER-SECOND", "BATCHES-PER-SECOND", "AVERAGE-BYTES-PER-BATCH")
		defer tw.Flush()

		for _, ts := range tss {
			tw.Print(ts.topic, ts.partitions, ts.throughput, ts.totalBatchesPerSecond, ts.averageBatchSize)
		}
	})

	sections.Add(secBatchRate, func() {
		tw := out.NewTable(append([]string{"TOPIC"}, percentileNames...)...)
		defer tw.Flush()
		for _, ps := range pss {
			vals := []any{}
			for _, pv := range ps.batchesPerSecond {
				vals = append(vals, pv.value)
			}

			if len(vals) == 0 {
				out.Die("empty vals")
			}

			tw.Print(append([]any{ps.topic}, vals...)...)
		}
	})

	sections.Add(secBatchSize, func() {
		tw := out.NewTable(append([]string{"TOPIC"}, percentileNames...)...)
		defer tw.Flush()
		for _, ps := range pss {
			vals := []any{}
			for _, pv := range ps.batchSizes {
				vals = append(vals, pv.value)
			}
			if len(vals) == 0 {
				out.Die("empty vals")
			}

			tw.Print(append([]any{ps.topic}, vals...)...)
		}
	})
	return nil
}

type kvTPInfo struct {
	TopicPartition topicPartition      `json:"topic_partition" yaml:"topic_partition"`
	Info           *topicPartitionInfo `json:"info" yaml:"info"`
}

type rawResults struct {
	TopicPartitionInfo []kvTPInfo `json:"topic_partition_info" yaml:"topic_partition_info"`
	TimeRange          timeRange  `json:"time_range" yaml:"time_range"`
}

func (cr *collectResult) printRawResults(f config.OutFormatter, w io.Writer) (bool, error) {
	// Json doesn't seem to like serializing the map types. So convert it to a slice here.
	kvs := []kvTPInfo{}
	for k, v := range cr.TopicPartitionInfo {
		kvs = append(kvs, kvTPInfo{k, v})
	}

	res := rawResults{kvs, cr.TimeRange}
	if isText, _, t, err := f.Format(res); !isText {
		if err != nil {
			return false, err
		}

		fmt.Fprintln(w, t)
		return true, nil
	}
	return false, nil
}

const helpAnalyze = `Analyze topics.

This command consumes records from the specified topics to determine
topic characteristics such as batch rate and batch size.

To print all of the metadata collected, use the --format flag with either JSON or YAML.

TOPICS

Topics can either be listed individually or by regex via the --regex flag (-r).

For example,

    analyze foo bar            # analyze topics foo and bar
    analyze -r '^f.*' '.*r$'   # analyze all topics starting with f and all topics ending in r
    analyze -r '*'             # analyze all topics
    analyze -r .               # analyze any one-character topics

TIME RANGE

The --time-range flag specifies the time range to consume from.
Use the following format:

    t1:t2    consume from timestamp t1 until timestamp t2

There are a few options for the timestamp syntax. 'rpk' evaluates each option
until one succeeds:

    13 digits             parsed as a Unix millisecond
    9 digits              parsed as a Unix second
    YYYY-MM-DD            parsed as a day, UTC
    YYYY-MM-DDTHH:MM:SSZ  parsed as RFC3339, UTC; fractional seconds optional (.MMM)
    end                   for t2 in @t1:t2, the current end of the partition
    -dur                  a negative duration from now or from a timestamp
    dur                   a positive duration from now or from a timestamp

Durations can be relative to the current time or relative to a timestamp.
If a duration is used for t1, that duration is relative to now.
If a duration is used for t2, and t1 is a timestamp, then t2 is relative to t1.
If a duration is used for t2, and t1 is a duration, then t2 is relative to now.

Durations are parsed simply:

    3ms    three milliseconds
    10s    ten seconds
    9m     nine minutes
    1h     one hour
    1m3ms  one minute and three milliseconds

For example,

    -t 2022-02-14:1h   consume 1h of time on Valentine's Day 2022
    -t -48h:-24h       consume from 2 days ago to 1 day ago
    -t -1m:end         consume from 1m ago until now
`
