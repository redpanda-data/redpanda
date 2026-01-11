package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var (
	// Version information (set via ldflags)
	Version   = "dev"
	GitCommit = "unknown"
	BuildDate = "unknown"
)

// Global flags
var (
	brokers        []string
	metricsAddress string
	metricsPort    int
)

var rootCmd = &cobra.Command{
	Use:   "client-swarm",
	Short: "Kafka workload generator for stress testing",
	Long: `client-swarm is a Kafka workload generator designed to stress-test
Kafka/Redpanda brokers with configurable producer and consumer swarms.`,
	Version: fmt.Sprintf("%s (commit: %s, built: %s)", Version, GitCommit, BuildDate),
}

func Execute() error {
	return rootCmd.Execute()
}

func init() {
	rootCmd.PersistentFlags().StringSliceVarP(&brokers, "brokers", "b", []string{"localhost:9092"}, "Kafka broker addresses")
	rootCmd.PersistentFlags().StringVar(&metricsAddress, "metrics-address", "127.0.0.1", "Address to bind metrics HTTP server")
	rootCmd.PersistentFlags().IntVar(&metricsPort, "metrics-port", 8080, "Port for metrics HTTP server")

	rootCmd.AddCommand(producersCmd)
	rootCmd.AddCommand(consumersCmd)
	rootCmd.AddCommand(connectionsCmd)
}

func exitWithError(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
