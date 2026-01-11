package cmd

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/redpanda-data/redpanda/tests/go/client-swarm/internal/connections"
	"github.com/spf13/cobra"
)

var connectionOpts connections.Options

var connectionsCmd = &cobra.Command{
	Use:   "connections",
	Short: "Test TCP connection backlog",
	Long:  `Stress test TCP listen backlog capacity by opening many concurrent connections.`,
	RunE:  runConnections,
}

func init() {
	connectionsCmd.Flags().IntVar(&connectionOpts.Number, "number", 100, "Number of concurrent connections")
}

func runConnections(cmd *cobra.Command, args []string) error {
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

	// Set brokers
	connectionOpts.Brokers = brokers

	// Run connection test
	stats, err := connections.Run(ctx, connectionOpts)
	if err != nil {
		return fmt.Errorf("connection test error: %w", err)
	}

	// Print results
	log.Printf("=== Connection Test Results ===")
	log.Printf("Total connections attempted: %d", stats.TotalAttempted)
	log.Printf("Successful connections: %d", stats.Successful)
	log.Printf("Failed connections: %d", stats.Failed)
	log.Printf("Failed writes: %d", stats.FailedWrites)

	if stats.Failed > 0 || stats.FailedWrites > 0 {
		return fmt.Errorf("connection test had failures")
	}

	return nil
}
