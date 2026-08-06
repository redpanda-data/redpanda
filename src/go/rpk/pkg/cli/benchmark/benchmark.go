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
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
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
	"golang.org/x/time/rate"
)

type stats struct {
	requests atomic.Uint64
	bytes    atomic.Uint64
	errors   atomic.Uint64
}

// latencyHistogram collects per-request latency samples for percentile
// computation. Samples are appended lock-free to per-goroutine slices
// and merged at the end.
type latencyHistogram struct {
	mu      sync.Mutex
	samples []float64 // microseconds
}

func (h *latencyHistogram) add(d time.Duration) {
	us := float64(d.Nanoseconds()) / 1e3
	h.mu.Lock()
	h.samples = append(h.samples, us)
	h.mu.Unlock()
}

func (h *latencyHistogram) percentile(p float64) float64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.samples) == 0 {
		return 0
	}
	sort.Float64s(h.samples)
	idx := int(math.Ceil(p/100*float64(len(h.samples)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(h.samples) {
		idx = len(h.samples) - 1
	}
	return h.samples[idx]
}

func (h *latencyHistogram) max() float64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.samples) == 0 {
		return 0
	}
	sort.Float64s(h.samples)
	return h.samples[len(h.samples)-1]
}

func (h *latencyHistogram) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.samples)
}

// cpuSnapshot captures user and system CPU time from /proc/self/stat.
type cpuSnapshot struct {
	userTicks   uint64
	systemTicks uint64
}

func readCPU() cpuSnapshot {
	f, err := os.Open("/proc/self/stat")
	if err != nil {
		return cpuSnapshot{}
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	if !scanner.Scan() {
		return cpuSnapshot{}
	}
	// /proc/self/stat fields: pid (comm) state ... field 14=utime, 15=stime
	line := scanner.Text()
	// Skip past the command name in parens.
	idx := strings.LastIndex(line, ") ")
	if idx < 0 {
		return cpuSnapshot{}
	}
	fields := strings.Fields(line[idx+2:])
	// After ") ", field 0=state, so utime=field[11], stime=field[12]
	if len(fields) < 13 {
		return cpuSnapshot{}
	}
	utime, _ := strconv.ParseUint(fields[11], 10, 64)
	stime, _ := strconv.ParseUint(fields[12], 10, 64)
	return cpuSnapshot{userTicks: utime, systemTicks: stime}
}

func (s cpuSnapshot) sub(other cpuSnapshot) (userSec, sysSec float64) {
	ticksPerSec := 100.0 // sysconf(_SC_CLK_TCK) is 100 on Linux
	userSec = float64(s.userTicks-other.userTicks) / ticksPerSec
	sysSec = float64(s.systemTicks-other.systemTicks) / ticksPerSec
	return
}

type finalMetrics struct {
	RequestsPerSec float64 `json:"requests_per_sec"`
	MBPerSec       float64 `json:"mb_per_sec"`
	Errors         uint64  `json:"errors"`
	P50LatencyUs   float64 `json:"p50_latency_us"`
	P99LatencyUs   float64 `json:"p99_latency_us"`
	P999LatencyUs  float64 `json:"p999_latency_us"`
	MaxLatencyUs   float64 `json:"max_latency_us"`
	CpuUserSec     float64 `json:"cpu_user_sec"`
	CpuSysSec      float64 `json:"cpu_sys_sec"`
}

type benchmarkConfig struct {
	topic                  string
	partitions             int32
	replicas               int16
	clients                int
	reset                  bool
	useExistingTopic       bool
	warmupS                int
	durationS              int
	metricsJSON            string
	waitLeadershipBalanced bool
	targetRateMBps         float64
	maxRecords             int64
}

type benchmarkTiming struct {
	warmup       time.Duration
	duration     time.Duration
	measureStart time.Time
	measureEnd   time.Time
	runCtx       context.Context
	cancel       context.CancelFunc
}

type benchmarkRun struct {
	cfg     benchmarkConfig
	profile *config.RpkProfile
	ctx     context.Context
	cancel  context.CancelFunc
	timing  benchmarkTiming
	adm     *kadm.Client
	topic   string
}

const (
	statsReqWidth = 12
	statsMBWidth  = 8
	statsLatWidth = 12
	statsErrWidth = 8
)

func (cfg *benchmarkConfig) addFlags(cmd *cobra.Command) {
	cmd.Flags().StringVar(&cfg.topic, "topic", "rpk-benchmark-topic", "Benchmark topic name")
	cmd.Flags().Int32VarP(&cfg.partitions, "partitions", "p", 18, "Number of partitions for benchmark topic creation (ignored with --use-existing-topic)")
	cmd.Flags().Int16VarP(&cfg.replicas, "replicas", "r", 3, "Replication factor for benchmark topic creation (ignored with --use-existing-topic)")
	cmd.Flags().IntVar(&cfg.clients, "clients", 16, "Number of benchmark client connections")
	cmd.Flags().BoolVar(&cfg.reset, "reset-topic", false, "Delete the benchmark topic first if it already exists")
	cmd.Flags().BoolVar(&cfg.useExistingTopic, "use-existing-topic", false, "Use the benchmark topic as-is without creating or deleting it")
	cmd.Flags().IntVar(&cfg.warmupS, "warmup", 10, "Warmup duration in seconds")
	cmd.Flags().IntVar(&cfg.durationS, "duration", 60, "Measurement duration in seconds")
	cmd.Flags().StringVar(&cfg.metricsJSON, "metrics-json", "", "Optional path to write final metrics JSON")
	cmd.Flags().BoolVar(&cfg.waitLeadershipBalanced, "wait-leadership-balanced", true, "Wait for topic leadership to become balanced before starting the benchmark")
	cmd.Flags().Float64Var(&cfg.targetRateMBps, "target-rate", 0, "Target throughput in MB/s (0 = unlimited)")
	cmd.Flags().Int64Var(&cfg.maxRecords, "max-records", 0, "Stop after this many records are transferred post-warmup (0 = run for --duration); when set, --duration acts as a safety timeout. Transfer a fixed volume with e.g. 1 GiB = 1073741824/record-size records")
	cmd.MarkFlagsMutuallyExclusive("reset-topic", "use-existing-topic")
}

func (cfg benchmarkConfig) validate() error {
	if cfg.reset && cfg.useExistingTopic {
		return fmt.Errorf("--use-existing-topic cannot be used with --reset-topic")
	}
	if !cfg.useExistingTopic {
		if cfg.partitions <= 0 {
			return fmt.Errorf("invalid --partitions %d, must be > 0", cfg.partitions)
		}
		if cfg.replicas <= 0 {
			return fmt.Errorf("invalid --replicas %d, must be > 0", cfg.replicas)
		}
	}
	if cfg.clients <= 0 {
		return fmt.Errorf("invalid --clients %d, must be > 0", cfg.clients)
	}
	if cfg.warmupS < 0 {
		return fmt.Errorf("invalid --warmup %d, must be >= 0", cfg.warmupS)
	}
	if cfg.durationS <= 0 {
		return fmt.Errorf("invalid --duration %d, must be > 0", cfg.durationS)
	}
	if cfg.maxRecords < 0 {
		return fmt.Errorf("invalid --max-records %d, must be >= 0", cfg.maxRecords)
	}
	return nil
}

// newRateLimiter returns a token-bucket rate limiter constrained to
// targetMBps megabytes per second, or nil if targetMBps <= 0.
func newRateLimiter(targetMBps float64) *rate.Limiter {
	if targetMBps <= 0 {
		return nil
	}
	bytesPerSec := targetMBps * 1024 * 1024
	burst := int(bytesPerSec)
	if burst < 1 {
		burst = 1
	}
	return rate.NewLimiter(rate.Limit(bytesPerSec), burst)
}

func computeMetrics(s *stats, now, measureStart time.Time, hist *latencyHistogram, cpuStart, cpuEnd cpuSnapshot) finalMetrics {
	elapsed := now.Sub(measureStart).Seconds()
	if elapsed <= 0 {
		elapsed = 1e-9
	}
	userSec, sysSec := cpuEnd.sub(cpuStart)
	return finalMetrics{
		RequestsPerSec: float64(s.requests.Load()) / elapsed,
		MBPerSec:       (float64(s.bytes.Load()) / (1024 * 1024)) / elapsed,
		Errors:         s.errors.Load(),
		P50LatencyUs:   hist.percentile(50),
		P99LatencyUs:   hist.percentile(99),
		P999LatencyUs:  hist.percentile(99.9),
		MaxLatencyUs:   hist.max(),
		CpuUserSec:     userSec,
		CpuSysSec:      sysSec,
	}
}

func printStatsHeader(tw *out.TabWriter) {
	tw.Print(
		fmt.Sprintf("%*s", statsReqWidth, "REQUESTS/S"),
		fmt.Sprintf("%*s", statsMBWidth, "MB/S"),
		fmt.Sprintf("%*s", statsLatWidth, "p99 LAT(us)"),
		fmt.Sprintf("%*s", statsErrWidth, "ERRORS"),
	)
	_ = tw.Flush()
}

func printStats(tw *out.TabWriter, s *stats, now, measureStart time.Time, hist *latencyHistogram) {
	elapsed := now.Sub(measureStart).Seconds()
	if elapsed <= 0 {
		elapsed = 1e-9
	}
	tw.Print(
		fmt.Sprintf("%12.2f", float64(s.requests.Load())/elapsed),
		fmt.Sprintf("%8.2f", (float64(s.bytes.Load())/(1024*1024))/elapsed),
		fmt.Sprintf("%12.0f", hist.percentile(99)),
		fmt.Sprintf("%8d", s.errors.Load()),
	)
	_ = tw.Flush()
}

func newBenchmarkRun(fs afero.Fs, p *config.Params, cmd *cobra.Command, cfg benchmarkConfig) (*benchmarkRun, error) {
	var err error
	run := &benchmarkRun{cfg: cfg}
	defer func() {
		if err != nil {
			run.Close()
		}
	}()
	run.profile, err = p.LoadVirtualProfile(fs)
	if err != nil {
		return nil, fmt.Errorf("rpk unable to load config: %w", err)
	}

	run.adm, err = kafka.NewAdmin(fs, run.profile)
	if err != nil {
		return nil, fmt.Errorf("unable to initialize admin kafka client: %w", err)
	}

	run.ctx, run.cancel = setupSignalContext(cmd)

	err = setupBenchmarkTopic(
		run.ctx,
		run.adm,
		cfg.topic,
		cfg.partitions,
		cfg.replicas,
		cfg.reset,
		cfg.useExistingTopic,
	)
	if err != nil {
		return nil, err
	}
	run.topic = cfg.topic

	if cfg.waitLeadershipBalanced {
		fmt.Printf("waiting for balanced leadership on topic=%s\n", cfg.topic)
		if err = waitForBalancedLeadership(run.ctx, run.adm, cfg.topic); err != nil {
			return nil, err
		}
	}

	run.timing = newBenchmarkTiming(run.ctx, cfg)

	return run, nil
}

func (r *benchmarkRun) Close() {
	if r.cancel != nil {
		r.cancel()
	}
	if r.timing.cancel != nil {
		r.timing.cancel()
	}
	if !r.cfg.useExistingTopic && r.topic != "" {
		if err := deleteBenchmarkTopic(context.Background(), r.adm, r.topic); err != nil {
			fmt.Printf("cleanup warning: unable to delete topic %q: %v\n", r.topic, err)
		}
	}
	if r.adm != nil {
		r.adm.Close()
	}
}

func setupSignalContext(cmd *cobra.Command) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(cmd.Context())

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	go func() {
		defer signal.Stop(sigCh)
		select {
		case <-sigCh:
			cancel()
		case <-ctx.Done():
		}
	}()

	return ctx, cancel
}

func createPayload(recordSize int) []byte {
	payload := make([]byte, recordSize)
	for i := range payload {
		payload[i] = 'x'
	}
	return payload
}

func newBenchmarkTiming(ctx context.Context, cfg benchmarkConfig) benchmarkTiming {
	warmup := time.Duration(cfg.warmupS) * time.Second
	duration := time.Duration(cfg.durationS) * time.Second
	measureStart := time.Now().Add(warmup)
	measureEnd := measureStart.Add(duration)
	runCtx, cancel := context.WithDeadline(ctx, measureEnd)

	return benchmarkTiming{
		warmup:       warmup,
		duration:     duration,
		measureStart: measureStart,
		measureEnd:   measureEnd,
		runCtx:       runCtx,
		cancel:       cancel,
	}
}

func writeMetricsJSON(path string, metrics finalMetrics) error {
	b, err := json.MarshalIndent(metrics, "", "  ")
	if err != nil {
		return fmt.Errorf("unable to marshal metrics json: %w", err)
	}
	if err := os.WriteFile(path, append(b, '\n'), 0o644); err != nil {
		return fmt.Errorf("unable to write metrics json to %q: %w", path, err)
	}
	return nil
}

func waitForWarmup(ctx context.Context, warmup time.Duration) {
	if warmup <= 0 {
		return
	}

	select {
	case <-ctx.Done():
	case <-time.After(warmup):
	}
}

func runBenchmarkReporter(
	ctx context.Context,
	timing benchmarkTiming,
	stats *stats,
	hist *latencyHistogram,
	metricsJSON string,
	wait func(),
) error {
	statsTable := out.NewTable()

	waitForWarmup(ctx, timing.warmup)
	cpuStart := readCPU()
	printStatsHeader(statsTable)

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-timing.runCtx.Done():
			// Measure elapsed to the actual stop, not measureEnd: with
			// --max-records the run ends early once the volume target is
			// hit, so using measureEnd would understate throughput. Clamp
			// to measureEnd so the duration-based path is unchanged (its
			// deadline fires a hair past measureEnd).
			endTime := time.Now()
			if endTime.After(timing.measureEnd) {
				endTime = timing.measureEnd
			}
			wait()
			cpuEnd := readCPU()
			if ctx.Err() == nil {
				printStats(statsTable, stats, endTime, timing.measureStart, hist)
			}
			if metricsJSON != "" {
				if err := writeMetricsJSON(metricsJSON, computeMetrics(stats, endTime, timing.measureStart, hist, cpuStart, cpuEnd)); err != nil {
					return err
				}
			}
			return nil
		case <-ticker.C:
			printStats(statsTable, stats, time.Now(), timing.measureStart, hist)
		}
	}
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
		return fmt.Errorf("benchmark topic %q already exists; choose a unique --topic, pass --use-existing-topic to reuse it, or pass --reset-topic to recreate it (destructive) - error: %w", topic, resp.Err)
	}
	if resp.Err != nil {
		return resp.Err
	}
	return nil
}

func setupBenchmarkTopic(
	ctx context.Context,
	adm *kadm.Client,
	topic string,
	partitions int32,
	replicas int16,
	reset bool,
	useExistingTopic bool,
) error {
	if useExistingTopic {
		return ensureBenchmarkTopicExists(ctx, adm, topic)
	}
	if reset {
		if err := deleteBenchmarkTopic(ctx, adm, topic); err != nil {
			return fmt.Errorf("unable to reset benchmark topic %q: %w", topic, err)
		}
	}
	return createBenchmarkTopic(ctx, adm, topic, partitions, replicas)
}

func ensureBenchmarkTopicExists(ctx context.Context, adm *kadm.Client, topic string) error {
	md, err := adm.Metadata(ctx, topic)
	if err != nil {
		return err
	}
	td, ok := md.Topics[topic]
	if !ok || errors.Is(td.Err, kerr.UnknownTopicOrPartition) {
		return fmt.Errorf(
			"benchmark topic %q does not exist; create it first or omit --use-existing-topic",
			topic,
		)
	}
	if td.Err != nil {
		return td.Err
	}
	return nil
}

func deleteBenchmarkTopic(ctx context.Context, adm *kadm.Client, topic string) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	resps, err := adm.DeleteTopics(ctx, topic)
	if err != nil {
		return err
	}
	resp, ok := resps[topic]
	if !ok {
		return fmt.Errorf("missing delete topic response for %q", topic)
	}
	if errors.Is(resp.Err, kerr.UnknownTopicOrPartition) {
		return nil
	} else if resp.Err != nil {
		return resp.Err
	}
	return nil
}

// checks whether leadership is balanced, aka:
// - all brokers have equal amount of leadership.
// - The above but some are leader for one more partition.
func leadershipBalanced(md kadm.Metadata, topic string) (bool, string) {
	td, ok := md.Topics[topic]
	if !ok {
		return false, "topic metadata not found"
	}
	if td.Err != nil {
		return false, td.Err.Error()
	}
	partitionCount := len(td.Partitions)

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
	basePartitionLeadersPerBroker := partitionCount / brokerCount
	moduloLeaderCount := partitionCount % brokerCount

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
) error {
	ctx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	var lastReason string
	for {
		md, err := adm.Metadata(ctx, topic)
		if err == nil {
			if ok, reason := leadershipBalanced(md, topic); ok {
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

func NewCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:    "benchmark",
		Short:  "Run a Kafka benchmark",
		Long:   "Load testing tool which stresses the broker by sending small batches with high request rate",
		Hidden: true,
	}

	cmd.AddCommand(newProduceCommand(fs, p))
	cmd.AddCommand(newConsumeCommand(fs, p))

	return cmd
}
