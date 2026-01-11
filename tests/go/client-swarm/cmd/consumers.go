package cmd

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redpanda-data/redpanda/tests/go/client-swarm/internal/consumers"
	"github.com/redpanda-data/redpanda/tests/go/client-swarm/internal/metrics"
	"github.com/redpanda-data/redpanda/tests/go/client-swarm/internal/server"
	"github.com/spf13/cobra"
)

var consumerOpts consumers.Options

var consumersCmd = &cobra.Command{
	Use:   "consumers",
	Short: "Run consumer swarm",
	Long:  `Spawn a configurable swarm of concurrent Kafka consumers to stress test the broker.`,
	RunE:  runConsumers,
}

func init() {
	consumersCmd.Flags().StringVar(&consumerOpts.Topic, "topic", "", "Topic name to consume from")
	consumersCmd.Flags().BoolVar(&consumerOpts.UniqueTopics, "unique-topics", false, "Each consumer gets unique topic(s)")
	consumersCmd.Flags().BoolVar(&consumerOpts.UniqueGroups, "unique-groups", false, "Each consumer gets unique consumer group")
	consumersCmd.Flags().StringVar(&consumerOpts.Group, "group", "client-swarm", "Consumer group name")
	consumersCmd.Flags().StringVar(&consumerOpts.StaticPrefix, "static-prefix", "", "Prefix for static group membership (enables static membership)")
	consumersCmd.Flags().IntVar(&consumerOpts.Count, "count", 1, "Number of consumer clients")
	consumersCmd.Flags().Int64Var(&consumerOpts.Messages, "messages", 0, "Target message count (0 = unlimited)")
	consumersCmd.Flags().StringSliceVar(&consumerOpts.Properties, "properties", nil, "Kafka client properties (key=value)")
	consumersCmd.Flags().DurationVar(&consumerOpts.ClientSpawnWait, "client-spawn-wait", 33*time.Millisecond, "Delay between spawning clients")
	consumersCmd.Flags().IntVar(&consumerOpts.TopicsPerClient, "topics-per-client", 1, "Topics per client when using unique-topics")
	consumersCmd.Flags().DurationVar(&consumerOpts.SessionTimeout, "session-timeout", 45*time.Second, "Session timeout")
	consumersCmd.Flags().DurationVar(&consumerOpts.RebalanceTimeout, "rebalance-timeout", 60*time.Second, "Rebalance timeout")

	consumersCmd.MarkFlagRequired("topic")
}

func runConsumers(cmd *cobra.Command, args []string) error {
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
	consumerOpts.Brokers = brokers

	// Run consumers
	stats, err := consumers.Run(ctx, consumerOpts, metricsCtx)
	if err != nil && ctx.Err() == nil {
		return fmt.Errorf("consumer error: %w", err)
	}

	// Print final stats
	log.Printf("=== Final Statistics ===")
	log.Printf("Total messages consumed: %d", stats.TotalMessages)
	log.Printf("Total bytes consumed: %d", stats.TotalBytes)
	log.Printf("Duration: %v", stats.Duration)
	if stats.Duration.Seconds() > 0 {
		log.Printf("Throughput: %.2f msg/s, %.2f MB/s",
			float64(stats.TotalMessages)/stats.Duration.Seconds(),
			float64(stats.TotalBytes)/stats.Duration.Seconds()/1024/1024)
	}

	return nil
}
