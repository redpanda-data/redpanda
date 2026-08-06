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
)

type consumeConfig struct {
	benchmarkConfig
}

func newConsumeCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cfg := consumeConfig{
		benchmarkConfig: benchmarkConfig{
			useExistingTopic: true,
		},
	}

	cmd := &cobra.Command{
		Use:   "consume",
		Short: "Run a Kafka consume benchmark",
		Long: `Consume benchmark that reads from a pre-populated topic at maximum speed.

The topic must already exist and contain data (e.g. from a prior produce
benchmark run). Use --use-existing-topic (enabled by default for consume).`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runConsumeBenchmark(fs, p, cmd, cfg)
		},
	}

	cfg.addFlags(cmd)
	// Force use-existing-topic for consume — the topic must already have data.
	cmd.Flags().Lookup("use-existing-topic").DefValue = "true"

	return cmd
}

func runConsumeBenchmark(fs afero.Fs, p *config.Params, cmd *cobra.Command, cfg consumeConfig) error {
	if err := cfg.validate(); err != nil {
		return err
	}

	run, err := newBenchmarkRun(fs, p, cmd, cfg.benchmarkConfig)
	if err != nil {
		return err
	}
	defer run.Close()

	stats := &stats{}
	hist := &latencyHistogram{}

	consumerOpts := []kgo.Opt{
		kgo.ConsumeTopics(cfg.topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxBytes(50 << 20),
		kgo.FetchMaxPartitionBytes(5 << 20),
	}

	consumerClients := make([]*kgo.Client, 0, cfg.clients)
	for i := 0; i < cfg.clients; i++ {
		cl, err := kafka.NewFranzClient(fs, run.profile, consumerOpts...)
		if err != nil {
			for _, started := range consumerClients {
				started.Close()
			}
			return fmt.Errorf("unable to initialize consumer client %d: %w", i, err)
		}
		consumerClients = append(consumerClients, cl)
	}
	defer func() {
		for _, cl := range consumerClients {
			cl.Close()
		}
	}()

	fmt.Printf(
		"mode=consume topic=%s clients=%d use_existing_topic=true\n",
		cfg.topic,
		cfg.clients,
	)
	if cfg.maxRecords > 0 {
		fmt.Printf("max_records=%d (fixed-volume mode; --duration is a safety timeout)\n", cfg.maxRecords)
	}
	// Skip warmup for consume — topics have finite data and warmup would
	// exhaust it, leaving nothing for the measurement period.
	run.timing.measureStart = time.Now()

	var wg sync.WaitGroup
	for _, cl := range consumerClients {
		wg.Add(1)
		go func(cl *kgo.Client) {
			defer wg.Done()
			runConsumerLoop(run.timing.runCtx, cl, run.timing.measureStart, stats, hist, cfg.maxRecords, run.timing.cancel)
		}(cl)
	}

	return runBenchmarkReporter(run.ctx, run.timing, stats, hist, cfg.metricsJSON, wg.Wait)
}

func runConsumerLoop(
	ctx context.Context,
	cl *kgo.Client,
	measureStart time.Time,
	stats *stats,
	hist *latencyHistogram,
	maxRecords int64,
	stop context.CancelFunc,
) {
	for {
		if ctx.Err() != nil {
			return
		}

		start := time.Now()
		fetches := cl.PollFetches(ctx)
		fetchDuration := time.Since(start)

		if ctx.Err() != nil {
			return
		}

		var fetchBytes int
		var fetchRecords int
		fetches.EachRecord(func(r *kgo.Record) {
			fetchBytes += len(r.Value)
			fetchRecords++
		})

		if time.Now().Before(measureStart) {
			continue
		}

		if errs := fetches.Errors(); len(errs) > 0 {
			stats.errors.Add(uint64(len(errs)))
		}

		if fetchRecords > 0 {
			hist.add(fetchDuration)
			n := stats.requests.Add(uint64(fetchRecords))
			stats.bytes.Add(uint64(fetchBytes))
			// Fixed-volume mode: stop once the target record count is
			// reached. stop() cancels runCtx for all consumer loops and
			// the reporter.
			if maxRecords > 0 && n >= uint64(maxRecords) {
				stop()
				return
			}
		}
	}
}
