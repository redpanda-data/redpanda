package cmd

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redpanda-data/redpanda/tests/go/client-swarm/internal/metrics"
	"github.com/redpanda-data/redpanda/tests/go/client-swarm/internal/producers"
	"github.com/redpanda-data/redpanda/tests/go/client-swarm/internal/server"
	"github.com/spf13/cobra"
)

var producerOpts producers.Options

var producersCmd = &cobra.Command{
	Use:   "producers",
	Short: "Run producer swarm",
	Long:  `Spawn a configurable swarm of concurrent Kafka producers to stress test the broker.`,
	RunE:  runProducers,
}

func init() {
	producersCmd.Flags().StringVar(&producerOpts.Topic, "topic", "", "Topic name to produce to")
	producersCmd.Flags().BoolVar(&producerOpts.UniqueTopics, "unique-topics", false, "Each producer gets unique topic(s)")
	producersCmd.Flags().IntVar(&producerOpts.Count, "count", 1, "Number of producer clients")
	producersCmd.Flags().Int64Var(&producerOpts.Messages, "messages", 0, "Messages per producer (0 = unlimited)")
	producersCmd.Flags().Float64Var(&producerOpts.MessagesPerSecond, "messages-per-second", 0, "Rate limit in messages per second (0 = unlimited)")
	producersCmd.Flags().DurationVar(&producerOpts.MessagePeriod, "message-period", 0, "Period between messages (alternative to messages-per-second)")
	producersCmd.Flags().StringSliceVar(&producerOpts.Properties, "properties", nil, "Kafka client properties (key=value)")
	producersCmd.Flags().StringVar(&producerOpts.CompressionType, "compression-type", "", "Compression type: none, gzip, snappy, lz4, zstd, or mixed")
	producersCmd.Flags().IntVar(&producerOpts.MinRecordSize, "min-record-size", 1024, "Minimum payload size in bytes")
	producersCmd.Flags().IntVar(&producerOpts.MaxRecordSize, "max-record-size", 1024, "Maximum payload size in bytes")
	producersCmd.Flags().BoolVar(&producerOpts.CompressiblePayload, "compressible-payload", false, "Use highly compressible payload data")
	producersCmd.Flags().Int64Var(&producerOpts.Keys, "keys", 1000, "Number of unique keys")
	producersCmd.Flags().DurationVar(&producerOpts.ClientSpawnWait, "client-spawn-wait", 33*time.Millisecond, "Delay between spawning clients")
	producersCmd.Flags().DurationVar(&producerOpts.Timeout, "timeout", 30*time.Second, "Message send timeout")
	producersCmd.Flags().IntVar(&producerOpts.TopicsPerClient, "topics-per-client", 1, "Topics per client when using unique-topics")
	producersCmd.Flags().StringVar(&producerOpts.PayloadDirectory, "payload-directory", "", "Directory with .data files for payloads")
	producersCmd.Flags().IntVar(&producerOpts.BatchMaxBytes, "batch-max-bytes", 1048576, "Maximum batch size in bytes")
	producersCmd.Flags().IntVar(&producerOpts.MaxBufferedRecords, "max-buffered-records", 10000, "Maximum buffered records per producer")
	producersCmd.Flags().StringVar(&producerOpts.Acks, "acks", "all", "Required acks: all, leader, none")

	producersCmd.MarkFlagRequired("topic")
}

func runProducers(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("Received shutdown signal")
		cancel()
	}()

	// Create metrics context
	metricsCtx := metrics.NewContext()

	// Start metrics HTTP server
	serverAddr := fmt.Sprintf("%s:%d", metricsAddress, metricsPort)
	go func() {
		if err := server.Start(ctx, serverAddr, metricsCtx); err != nil {
			log.Printf("Metrics server error: %v", err)
		}
	}()

	log.Printf("Starting metrics server on %s", serverAddr)

	// Set brokers
	producerOpts.Brokers = brokers

	// Run producers
	stats, err := producers.Run(ctx, producerOpts, metricsCtx)
	if err != nil && ctx.Err() == nil {
		return fmt.Errorf("producer error: %w", err)
	}

	// Print final stats
	log.Printf("=== Final Statistics ===")
	log.Printf("Total messages sent: %d", stats.TotalMessages)
	log.Printf("Total errors: %d", stats.TotalErrors)
	log.Printf("Total bytes sent: %d", stats.TotalBytes)
	log.Printf("Duration: %v", stats.Duration)
	if stats.Duration.Seconds() > 0 {
		log.Printf("Throughput: %.2f msg/s, %.2f MB/s",
			float64(stats.TotalMessages)/stats.Duration.Seconds(),
			float64(stats.TotalBytes)/stats.Duration.Seconds()/1024/1024)
	}
	log.Printf("Min rate: %.2f msg/s", stats.MinRate)
	log.Printf("Max rate: %.2f msg/s", stats.MaxRate)
	log.Printf("Avg rate: %.2f msg/s", stats.AvgRate)

	return nil
}
