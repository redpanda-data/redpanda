// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package benchmark

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/kafka"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type fetchMeasurementHooks struct {
	measureStart time.Time
	measureEnd   time.Time
	stats        *stats
}

type fetchConfig struct {
	benchmarkConfig
	recordSize               int
	prefillBytesPerPartition int64
}

const (
	defaultFetchRecordSize               = 10 << 10
	defaultFetchPrefillBytesPerPartition = 1 << 30
	fetchMaxWait                         = 10 * time.Millisecond
	fetchPartitionBytesSlack             = 1024
)

var fetchRequestKey = kmsg.NewPtrFetchRequest().Key()

func (h *fetchMeasurementHooks) OnBrokerE2E(_ kgo.BrokerMetadata, key int16, e2e kgo.BrokerE2E) {
	if key != fetchRequestKey || !withinMeasurementWindow(time.Now(), h.measureStart, h.measureEnd) {
		return
	}

	h.stats.requests.Add(1)
	h.stats.bytes.Add(uint64(e2e.BytesRead))
	if e2e.Err() != nil {
		h.stats.errors.Add(1)
	}
}

var _ kgo.HookBrokerE2E = (*fetchMeasurementHooks)(nil)

func newFetchCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var cfg fetchConfig

	cmd := &cobra.Command{
		Use:    "fetch",
		Short:  "Run a Kafka fetch benchmark",
		Long:   "Load testing tool which pre-fills a topic and repeatedly polls fetches with deterministic direct partition assignments to avoid consumer-group rebalance noise and to allow explicit rewind-to-start loops once the prefilled data is drained",
		Args:   cobra.NoArgs,
		Hidden: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runFetchBenchmark(fs, p, cmd, cfg)
		},
	}

	cfg.addFlags(cmd)
	cmd.Flags().IntVar(&cfg.recordSize, "record-size", defaultFetchRecordSize, "Record size in bytes; for fetch this also determines the per-fetch batch size target")
	cmd.Flags().Int64Var(&cfg.prefillBytesPerPartition, "prefill-bytes-per-partition", defaultFetchPrefillBytesPerPartition, "Number of bytes to prefill in each partition before the fetch benchmark starts")

	return cmd
}

func runFetchBenchmark(fs afero.Fs, p *config.Params, cmd *cobra.Command, cfg fetchConfig) error {
	if err := cfg.validate(); err != nil {
		return err
	}
	if cfg.recordSize <= 0 {
		return fmt.Errorf("invalid --record-size %d, must be > 0", cfg.recordSize)
	}
	if cfg.prefillBytesPerPartition <= 0 {
		return fmt.Errorf("invalid --prefill-bytes-per-partition %d, must be > 0", cfg.prefillBytesPerPartition)
	}

	run, err := newBenchmarkRun(fs, p, cmd, cfg.benchmarkConfig)
	if err != nil {
		return err
	}
	defer run.close()

	payload := createPayload(cfg.recordSize)
	recordsPerPartition, err := fetchPrefillRecordsPerPartition(cfg.recordSize, cfg.prefillBytesPerPartition)
	if err != nil {
		return err
	}
	fmt.Printf(
		"prefilling topic=%s bytes_per_partition=%d records_per_partition=%d record_size=%d\n",
		cfg.topic,
		cfg.prefillBytesPerPartition,
		recordsPerPartition,
		cfg.recordSize,
	)
	if err := prefillFetchTopic(run.ctx, fs, run.profile, cfg.topic, cfg.partitions, payload, recordsPerPartition); err != nil {
		return err
	}

	stats := &stats{}

	assignments := buildFetchPartitionAssignments(cfg.partitions, cfg.clients)
	fetchPartitionMaxBytes := fetchMaxPartitionBytes(cfg.recordSize)
	rewindOffset := kgo.NewOffset().AtStart().EpochOffset()

	fetchClients := make([]*kgo.Client, 0, cfg.clients)
	for i, assignment := range assignments {
		hooks := &fetchMeasurementHooks{
			measureStart: run.timing.measureStart,
			measureEnd:   run.timing.measureEnd,
			stats:        stats,
		}
		cl, err := kafka.NewFranzClient(
			fs,
			run.profile,
			kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{cfg.topic: assignment}),
			kgo.FetchMinBytes(1),
			kgo.FetchMaxWait(fetchMaxWait),
			kgo.FetchMaxPartitionBytes(fetchPartitionMaxBytes),
			kgo.WithHooks(hooks),
		)
		if err != nil {
			for _, started := range fetchClients {
				started.Close()
			}
			return fmt.Errorf("unable to initialize fetch client %d: %w", i, err)
		}
		fetchClients = append(fetchClients, cl)
	}
	defer func() {
		for _, cl := range fetchClients {
			cl.Close()
		}
	}()

	fmt.Printf(
		"mode=fetch topic=%s clients=%d partitions=%d record_size=%d replication_factor=%d prefill_bytes_per_partition=%d records_per_partition=%d fetch_max_partition_bytes=%d\n",
		cfg.topic,
		cfg.clients,
		cfg.partitions,
		cfg.recordSize,
		cfg.replicas,
		cfg.prefillBytesPerPartition,
		recordsPerPartition,
		fetchPartitionMaxBytes,
	)
	if run.timing.warmup > 0 {
		fmt.Printf("warming up for %ds...\n", cfg.warmupS)
	}

	var wg sync.WaitGroup
	for _, cl := range fetchClients {
		wg.Add(1)
		go func(cl *kgo.Client) {
			defer wg.Done()
			runFetchLoop(run.timing.runCtx, cl, rewindOffset, run.timing.measureStart, run.timing.measureEnd, stats)
		}(cl)
	}

	return runBenchmarkReporter(run.ctx, run.timing, stats, cfg.metricsJSON, wg.Wait)
}

func fetchPrefillRecordsPerPartition(recordSize int, bytesPerPartition int64) (int, error) {
	records := (bytesPerPartition + int64(recordSize) - 1) / int64(recordSize)
	if records < 1 {
		return 1, nil
	}
	maxInt := int64(^uint(0) >> 1)
	if records > maxInt {
		return 0, fmt.Errorf("prefill records per partition %d exceed supported limit %d", records, maxInt)
	}
	return int(records), nil
}

func fetchMaxPartitionBytes(recordSize int) int32 {
	maxInt32 := int(^uint32(0) >> 1)
	if recordSize >= maxInt32-fetchPartitionBytesSlack {
		return int32(maxInt32)
	}
	return int32(recordSize + fetchPartitionBytesSlack)
}

// Fetch mode uses direct assignments instead of consumer groups so that the
// benchmark avoids rebalance / group-protocol noise, keeps all configured
// clients active with deterministic placement, and can rewind partitions
// explicitly after draining the prefilled topic data.
func buildFetchPartitionAssignments(partitions int32, clients int) []map[int32]kgo.Offset {
	if partitions <= 0 || clients <= 0 {
		return nil
	}

	assignments := make([]map[int32]kgo.Offset, clients)
	startOffset := kgo.NewOffset().AtStart()
	for i := range assignments {
		assignments[i] = make(map[int32]kgo.Offset)
	}

	for partition := int32(0); partition < partitions; partition++ {
		assignments[int(partition)%clients][partition] = startOffset
	}

	for client := range assignments {
		if len(assignments[client]) == 0 {
			assignments[client][int32(client%int(partitions))] = startOffset
		}
	}

	return assignments
}

// Prefill enough records that fetch clients can immediately hit a steady-state
// poll loop before they begin rewinding back to offset 0.
func prefillFetchTopic(
	ctx context.Context,
	fs afero.Fs,
	profile *config.RpkProfile,
	topic string,
	partitions int32,
	payload []byte,
	recordsPerPartition int,
) error {
	cl, err := kafka.NewFranzClient(
		fs,
		profile,
		kgo.DefaultProduceTopic(topic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordPartitioner(kgo.RoundRobinPartitioner()),
		kgo.ProducerBatchCompression(kgo.NoCompression()),
	)
	if err != nil {
		return fmt.Errorf("unable to initialize fetch prefill client: %w", err)
	}
	defer cl.Close()

	totalRecords := int(partitions) * recordsPerPartition
	for i := 0; i < totalRecords; i++ {
		err := cl.ProduceSync(ctx, &kgo.Record{
			Topic: topic,
			Value: payload,
		}).FirstErr()
		if err != nil {
			return fmt.Errorf("unable to prefill fetch benchmark topic: %w", err)
		}
	}
	return nil
}

// PollFetches is franz-go's blocking fetch API; there is no ProduceSync-style
// helper for fetches. We keep direct assignments so the benchmark can control
// placement and rewind offsets deterministically.
func runFetchLoop(
	ctx context.Context,
	cl *kgo.Client,
	rewindOffset kgo.EpochOffset,
	measureStart time.Time,
	measureEnd time.Time,
	stats *stats,
) {
	for {
		if ctx.Err() != nil {
			return
		}

		fetches := cl.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return
		}

		fetches.EachError(func(_ string, _ int32, _ error) {
			if withinMeasurementWindow(time.Now(), measureStart, measureEnd) && ctx.Err() == nil {
				stats.errors.Add(1)
			}
		})

		rewinds := make(map[string]map[int32]kgo.EpochOffset)
		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			if p.Err != nil || len(p.Records) != 0 || p.HighWatermark == 0 {
				return
			}

			topicRewinds := rewinds[p.Topic]
			if topicRewinds == nil {
				topicRewinds = make(map[int32]kgo.EpochOffset)
				rewinds[p.Topic] = topicRewinds
			}
			topicRewinds[p.Partition] = rewindOffset
		})

		if len(rewinds) > 0 {
			cl.SetOffsets(rewinds)
		}
	}
}
