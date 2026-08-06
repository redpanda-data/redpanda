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
	"golang.org/x/time/rate"
)

type produceConfig struct {
	benchmarkConfig
	recordSize int
}

func newProduceCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var cfg produceConfig

	cmd := &cobra.Command{
		Use:   "produce",
		Short: "Run a Kafka produce benchmark",
		Long:  "Load testing tool which stresses the broker by sending small batches with high request rate",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runProduceBenchmark(fs, p, cmd, cfg)
		},
	}

	cfg.addFlags(cmd)
	cmd.Flags().IntVar(&cfg.recordSize, "record-size", 100, "Record payload size in bytes")

	return cmd
}

func runProduceBenchmark(fs afero.Fs, p *config.Params, cmd *cobra.Command, cfg produceConfig) error {
	if err := cfg.validate(); err != nil {
		return err
	}
	if cfg.recordSize <= 0 {
		return fmt.Errorf("invalid --record-size %d, must be > 0", cfg.recordSize)
	}

	run, err := newBenchmarkRun(fs, p, cmd, cfg.benchmarkConfig)
	if err != nil {
		return err
	}
	defer run.Close()

	payload := createPayload(cfg.recordSize)
	stats := &stats{}
	hist := &latencyHistogram{}
	limiter := newRateLimiter(cfg.targetRateMBps)

	// Cap buffered records so we don't exhaust disk on a single-node
	// broker. Target ~100 MB buffered per client.
	maxBuf := max(100, min(50000, 100*1024*1024/cfg.recordSize))

	producerOpts := []kgo.Opt{
		kgo.DefaultProduceTopic(cfg.topic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordPartitioner(kgo.RoundRobinPartitioner()),
		kgo.ProducerLinger(5 * time.Millisecond),
		kgo.ProducerBatchCompression(kgo.NoCompression()),
		kgo.MaxBufferedRecords(maxBuf),
	}

	producerClients := make([]*kgo.Client, 0, cfg.clients)
	for i := 0; i < cfg.clients; i++ {
		cl, err := kafka.NewFranzClient(fs, run.profile, producerOpts...)
		if err != nil {
			for _, started := range producerClients {
				started.Close()
			}
			return fmt.Errorf("unable to initialize producer client %d: %w", i, err)
		}
		producerClients = append(producerClients, cl)
	}
	defer func() {
		for _, cl := range producerClients {
			cl.Close()
		}
	}()

	if cfg.useExistingTopic {
		fmt.Printf(
			"mode=produce topic=%s clients=%d record_size=%d use_existing_topic=true\n",
			cfg.topic,
			cfg.clients,
			cfg.recordSize,
		)
	} else {
		fmt.Printf(
			"mode=produce topic=%s clients=%d partitions=%d record_size=%d replication_factor=%d\n",
			cfg.topic,
			cfg.clients,
			cfg.partitions,
			cfg.recordSize,
			cfg.replicas,
		)
	}
	if cfg.maxRecords > 0 {
		fmt.Printf("max_records=%d (fixed-volume mode; --duration is a safety timeout)\n", cfg.maxRecords)
	}
	if run.timing.warmup > 0 {
		fmt.Printf("warming up for %ds...\n", cfg.warmupS)
	}

	var wg sync.WaitGroup
	for _, cl := range producerClients {
		wg.Add(1)
		go func(cl *kgo.Client) {
			defer wg.Done()
			runProducerLoop(run.timing.runCtx, cl, cfg.topic, payload, run.timing.measureStart, stats, hist, limiter, cfg.maxRecords, run.timing.cancel)
		}(cl)
	}

	return runBenchmarkReporter(run.ctx, run.timing, stats, hist, cfg.metricsJSON, wg.Wait)
}

func runProducerLoop(
	ctx context.Context,
	cl *kgo.Client,
	topic string,
	payload []byte,
	measureStart time.Time,
	stats *stats,
	hist *latencyHistogram,
	limiter *rate.Limiter,
	maxRecords int64,
	stop context.CancelFunc,
) {
	for {
		if ctx.Err() != nil {
			break
		}

		if limiter != nil {
			if err := limiter.WaitN(ctx, len(payload)); err != nil {
				break
			}
		}

		rec := &kgo.Record{Topic: topic, Value: payload}
		start := time.Now()
		payloadLen := uint64(len(payload))

		cl.Produce(ctx, rec, func(_ *kgo.Record, err error) {
			if time.Now().Before(measureStart) {
				return
			}
			if err != nil {
				if ctx.Err() == nil {
					stats.requests.Add(1)
					stats.errors.Add(1)
				}
				return
			}
			hist.add(time.Since(start))
			n := stats.requests.Add(1)
			stats.bytes.Add(payloadLen)
			// Fixed-volume mode: stop the whole run once the target record
			// count is reached. stop() cancels runCtx, which every producer
			// loop and the reporter observe.
			if maxRecords > 0 && n >= uint64(maxRecords) {
				stop()
			}
		})
	}
	// Flush remaining buffered records before exiting.
	cl.Flush(context.Background())
}
