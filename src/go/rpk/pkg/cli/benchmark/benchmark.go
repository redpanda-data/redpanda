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
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/kafka"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

type stats struct {
	requests atomic.Uint64
	bytes    atomic.Uint64
	errors   atomic.Uint64
}

type finalMetrics struct {
	RequestsPerSec float64 `json:"requests_per_sec"`
	MBPerSec       float64 `json:"mb_per_sec"`
	Errors         uint64  `json:"errors"`
}

const (
	statsReqWidth = 12
	statsMBWidth  = 8
	statsErrWidth = 8
)

func computeMetrics(s *stats, now, measureStart time.Time) finalMetrics {
	elapsed := now.Sub(measureStart).Seconds()
	if elapsed <= 0 {
		elapsed = 1e-9
	}
	return finalMetrics{
		RequestsPerSec: float64(s.requests.Load()) / elapsed,
		MBPerSec:       (float64(s.bytes.Load()) / (1024 * 1024)) / elapsed,
		Errors:         s.errors.Load(),
	}
}

func printStatsHeader(tw *out.TabWriter) {
	tw.Print(
		fmt.Sprintf("%*s", statsReqWidth, "REQUESTS/S"),
		fmt.Sprintf("%*s", statsMBWidth, "MB/S"),
		fmt.Sprintf("%*s", statsErrWidth, "ERRORS"),
	)
	_ = tw.Flush()
}

func printStats(tw *out.TabWriter, s *stats, now, measureStart time.Time) {
	m := computeMetrics(s, now, measureStart)

	tw.Print(
		fmt.Sprintf("%12.2f", m.RequestsPerSec),
		fmt.Sprintf("%8.2f", m.MBPerSec),
		fmt.Sprintf("%8d", m.Errors),
	)
	_ = tw.Flush()
}

func createBenchmarkTopic(ctx context.Context, adm *kadm.Client, topic string, partitions int32, replicas int16) error {
	resps, err := adm.CreateTopics(ctx, partitions, replicas, nil, topic)
	if err != nil {
		return err
	}
	resp, ok := resps[topic]
	if !ok {
		return fmt.Errorf("missing create topic response for %q", topic)
	}
	if errors.Is(resp.Err, kerr.TopicAlreadyExists) {
		return fmt.Errorf("benchmark topic %q already exists; choose a unique --topic", topic)
	}
	if resp.Err != nil {
		return resp.Err
	}
	return nil
}

func deleteBenchmarkTopic(adm *kadm.Client, topic string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resps, err := adm.DeleteTopics(ctx, topic)
	if err != nil {
		return err
	}
	resp, ok := resps[topic]
	if !ok {
		return fmt.Errorf("missing delete topic response for %q", topic)
	}
	if resp.Err != nil {
		return resp.Err
	}
	return nil
}

// checks whether leadership is balanced, aka:
// - all brokers have equal amount of leadership.
// - The above but some are leader for one more partition.
func leadershipBalanced(md kadm.Metadata, topic string, expectedPartitions int32) (bool, string) {
	td, ok := md.Topics[topic]
	if !ok {
		return false, "topic metadata not found"
	}
	if td.Err != nil {
		return false, td.Err.Error()
	}
	if int32(len(td.Partitions)) != expectedPartitions {
		return false, fmt.Sprintf(
			"partition count is %d, expected %d",
			len(td.Partitions),
			expectedPartitions,
		)
	}

	replicaNodes := make(map[int32]struct{})
	leaderCounts := make(map[int32]int)
	for _, p := range td.Partitions {
		if p.Err != nil {
			return false, p.Err.Error()
		}
		if p.Leader < 0 {
			return false, fmt.Sprintf("partition %d has no leader", p.Partition)
		}
		leaderCounts[p.Leader]++
		for _, replica := range p.Replicas {
			replicaNodes[replica] = struct{}{}
		}
	}

	if len(replicaNodes) == 0 {
		return false, "no replicas in topic metadata"
	}

	brokerCount := len(replicaNodes)
	basePartitionLeadersPerBroker := int(expectedPartitions) / brokerCount
	moduloLeaderCount := int(expectedPartitions) % brokerCount

	brokersAtBase := 0
	brokersAtBasePlusOne := 0
	for replica := range replicaNodes {
		count := leaderCounts[replica]
		switch count {
		case basePartitionLeadersPerBroker:
			brokersAtBase++
		case basePartitionLeadersPerBroker + 1:
			brokersAtBasePlusOne++
		default:
			return false, fmt.Sprintf(
				"leader distribution invalid: broker %d has %d leaders (expected %d or %d)",
				replica,
				count,
				basePartitionLeadersPerBroker,
				basePartitionLeadersPerBroker+1,
			)
		}
	}

	if brokersAtBasePlusOne != moduloLeaderCount {
		return false, fmt.Sprintf(
			"leader distribution invalid: expected %d brokers with %d leaders, got %d",
			moduloLeaderCount,
			basePartitionLeadersPerBroker+1,
			brokersAtBasePlusOne,
		)
	}
	if brokersAtBase != brokerCount-moduloLeaderCount {
		return false, fmt.Sprintf(
			"leader distribution invalid: expected %d brokers with %d leaders, got %d",
			brokerCount-moduloLeaderCount,
			basePartitionLeadersPerBroker,
			brokersAtBase,
		)
	}

	return true, ""
}

func waitForBalancedLeadership(
	ctx context.Context,
	adm *kadm.Client,
	topic string,
	expectedPartitions int32,
) error {
	ctx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	var lastReason string
	for {
		md, err := adm.Metadata(ctx, topic)
		if err == nil {
			if ok, reason := leadershipBalanced(md, topic, expectedPartitions); ok {
				return nil
			} else {
				lastReason = reason
			}
		} else {
			lastReason = err.Error()
		}

		select {
		case <-ctx.Done():
			if lastReason == "" {
				lastReason = ctx.Err().Error()
			}
			return fmt.Errorf(
				"timed out waiting for balanced leadership on topic %q: %s",
				topic,
				lastReason,
			)
		case <-ticker.C:
		}
	}
}

func runProducerLoop(
	ctx context.Context,
	cl *kgo.Client,
	topic string,
	payload []byte,
	measureStart time.Time,
	stats *stats,
) {
	for {
		if ctx.Err() != nil {
			return
		}

		rec := &kgo.Record{Topic: topic, Value: payload}

		// We use sync produce. Like this we can guarantee single record per batch per request.
		// To increase inflight it's easy to just bump clients/connections (this is cheap in franz-go)
		err := cl.ProduceSync(ctx, rec).FirstErr()
		now := time.Now()
		if now.Before(measureStart) {
			continue
		}
		if err != nil {
			if ctx.Err() == nil {
				stats.requests.Add(1)
				stats.errors.Add(1)
			}
			continue
		}

		stats.requests.Add(1)
		stats.bytes.Add(uint64(len(payload)))
	}
}

func NewCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:    "benchmark",
		Short:  "Run a Kafka benchmark",
		Long:   "Load testing tool which stresses the broker by sending small batches with high request rate",
		Hidden: true,
	}

	cmd.AddCommand(newProduceCommand(fs, p))

	return cmd
}

func newProduceCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var (
		topic                  string
		partitions             int32
		replicas               int16
		clients                int
		recordSize             int
		warmupS                int
		durationS              int
		metricsJSON            string
		waitLeadershipBalanced bool
	)

	cmd := &cobra.Command{
		Use:    "produce",
		Short:  "Run a Kafka produce benchmark",
		Long:   "Load testing tool which stresses the broker by sending small batches with high request rate",
		Args:   cobra.NoArgs,
		Hidden: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if partitions <= 0 {
				return fmt.Errorf("invalid --partitions %d, must be > 0", partitions)
			}
			if replicas <= 0 {
				return fmt.Errorf("invalid --replicas %d, must be > 0", replicas)
			}
			if clients <= 0 {
				return fmt.Errorf("invalid --clients %d, must be > 0", clients)
			}
			if recordSize <= 0 {
				return fmt.Errorf("invalid --record-size %d, must be > 0", recordSize)
			}
			if warmupS < 0 {
				return fmt.Errorf("invalid --warmup %d, must be >= 0", warmupS)
			}
			if durationS <= 0 {
				return fmt.Errorf("invalid --duration %d, must be > 0", durationS)
			}

			profile, err := p.LoadVirtualProfile(fs)
			if err != nil {
				return fmt.Errorf("rpk unable to load config: %w", err)
			}

			adm, err := kafka.NewAdmin(fs, profile)
			if err != nil {
				return fmt.Errorf("unable to initialize admin kafka client: %w", err)
			}
			defer adm.Close()

			ctx, cancel := context.WithCancel(cmd.Context())
			defer cancel()

			sigCh := make(chan os.Signal, 1)
			signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
			defer signal.Stop(sigCh)
			go func() {
				select {
				case <-sigCh:
					cancel()
				case <-ctx.Done():
				}
			}()

			err = createBenchmarkTopic(ctx, adm, topic, partitions, replicas)
			if err != nil {
				return err
			}
			defer func() {
				if err := deleteBenchmarkTopic(adm, topic); err != nil {
					fmt.Printf("cleanup warning: unable to delete topic %q: %v\n", topic, err)
				}
			}()

			if waitLeadershipBalanced {
				fmt.Printf("waiting for balanced leadership on topic=%s\n", topic)
				err = waitForBalancedLeadership(ctx, adm, topic, partitions)
				if err != nil {
					return err
				}
			}

			warmup := time.Duration(warmupS) * time.Second
			duration := time.Duration(durationS) * time.Second
			measureStart := time.Now().Add(warmup)
			measureEnd := measureStart.Add(duration)

			runCtx, runCancel := context.WithDeadline(ctx, measureEnd)
			defer runCancel()

			payload := make([]byte, recordSize)
			for i := range payload {
				payload[i] = 'x'
			}
			stats := &stats{}

			producerOpts := []kgo.Opt{
				kgo.DefaultProduceTopic(topic),
				kgo.RequiredAcks(kgo.AllISRAcks()),
				kgo.RecordPartitioner(kgo.RoundRobinPartitioner()),
				kgo.ProducerLinger(0),
				kgo.ProducerBatchCompression(kgo.NoCompression()),
			}

			producerClients := make([]*kgo.Client, 0, clients)
			for i := 0; i < clients; i++ {
				cl, err := kafka.NewFranzClient(fs, profile, producerOpts...)
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

			fmt.Printf("topic=%s clients=%d partitions=%d record_size=%d replication_factor=%d\n", topic, clients, partitions, recordSize, replicas)
			if warmup > 0 {
				fmt.Printf("warming up for %ds...\n", warmupS)
			}

			statsTable := out.NewTable()

			var wg sync.WaitGroup
			for _, cl := range producerClients {
				wg.Add(1)
				go func(cl *kgo.Client) {
					defer wg.Done()
					runProducerLoop(runCtx, cl, topic, payload, measureStart, stats)
				}(cl)
			}

			select {
			case <-runCtx.Done():
			case <-time.After(warmup):
			}

			printStatsHeader(statsTable)

			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-runCtx.Done():
					wg.Wait()
					if ctx.Err() == nil {
						printStats(statsTable, stats, measureEnd, measureStart)
					}
					if metricsJSON != "" {
						metrics := computeMetrics(stats, measureEnd, measureStart)
						b, err := json.MarshalIndent(metrics, "", "  ")
						if err != nil {
							return fmt.Errorf("unable to marshal metrics json: %w", err)
						}
						err = os.WriteFile(metricsJSON, append(b, '\n'), 0o644)
						if err != nil {
							return fmt.Errorf("unable to write metrics json to %q: %w", metricsJSON, err)
						}
					}
					return nil
				case <-ticker.C:
					printStats(statsTable, stats, time.Now(), measureStart)
				}
			}
		},
	}

	cmd.Flags().StringVar(&topic, "topic", "rpk-benchmark-topic", "Benchmark topic name")
	cmd.Flags().Int32VarP(&partitions, "partitions", "p", 18, "Number of partitions for benchmark topic creation")
	cmd.Flags().Int16VarP(&replicas, "replicas", "r", 3, "Replication factor for benchmark topic creation")
	cmd.Flags().IntVar(&clients, "clients", 16, "Number of producer client connections")
	cmd.Flags().IntVar(&recordSize, "record-size", 100, "Record payload size in bytes")
	cmd.Flags().IntVar(&warmupS, "warmup", 10, "Warmup duration in seconds")
	cmd.Flags().IntVar(&durationS, "duration", 60, "Measurement duration in seconds")
	cmd.Flags().StringVar(&metricsJSON, "metrics-json", "", "Optional path to write final metrics JSON")
	cmd.Flags().BoolVar(&waitLeadershipBalanced, "wait-leadership-balanced", true, "Wait for topic leadership to become balanced before producing")

	return cmd
}
