// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package selftest

import (
	"fmt"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/api/admin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"

	"github.com/docker/go-units"

	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func NewStartCommand(fs afero.Fs) *cobra.Command {
	var (
		noConfirm      bool
		diskDurationMs uint
		netDurationMs  uint
		onNodes        []int
		onlyDisk       bool
		onlyNetwork    bool
	)
	cmd := &cobra.Command{
		Use:   "start",
		Short: "Starts a new self test run",
		Long: `Starts one or more pre-canned tests on a selection or all nodes
of the cluster. Current tests that will run are:

* Disk tests:
  * Throughput test: 512k r/w sequential
    * Higher request sizes and deeper io queue depth will sacrifice IOPS /
      latency numbers in order to write / read more bytes in a shorter amount
      of time.
  * Latency test: 4k r/w sequential
    * Smaller request sizes and lower levels of parallelism to achieve higher
      IOPS and lower latency results.

* Network tests:
  * 8192b throughput test
    * Unique pairs of redpanda nodes will act as either a client or server.
    * Benchmark attempts to push as much data over the wire within the test
      parameters.

This command will immediately return once invoked with success. It is up to the
user to periodically check back for the result set using the 'self-test status'
command.`,
		Args: cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, _ []string) {
			// Load config settings
			p := config.ParamsFromCommand(cmd)
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			// Warn user before continuing, proceed only via explicit signal
			if !noConfirm {
				confirmed, err := out.Confirm("Invoking the redpanda self tests may put excessive pressure on your system. It attempts to benchmark disk and network hardware to find their max throughputs. Its advised to not start the test if there are already large workloads running on the system.")
				out.MaybeDie(err, "unable to confirm user confirmation: %v", err)
				if !confirmed {
					out.Exit("self-test start was cancelled")
				}
			}

			// Create new HTTP client for communication w/ admin server
			cl, err := admin.NewClient(fs, cfg)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			// Using cmd line args, assemble self_test_start_request body
			tests := assembleTests(onlyDisk, onlyNetwork, diskDurationMs, netDurationMs)

			// Make HTTP POST request to leader that starts the actual test
			tid, err := cl.StartSelfTest(cmd.Context(), onNodes, tests)
			out.MaybeDie(err, "unable to start self test: %v", err)
			fmt.Printf("Redpanda self-test has started, test identifier: %v, To check the status run:\n    rpk redpanda admin self-test status\n", tid)
		},
	}

	// Collect arguments via command line
	cmd.Flags().BoolVar(&noConfirm, "no-confirm", false, "Acknowledge warning prompt skipping read from stdin")
	cmd.Flags().UintVar(&diskDurationMs, "disk-duration-ms", 30000,
		"The duration in milliseconds of individual disk test runs")
	cmd.Flags().UintVar(&netDurationMs, "network-duration-ms", 30000,
		"The duration in milliseconds of individual disk test runs")
	cmd.Flags().IntSliceVar(&onNodes, "participant-node-ids", nil,
		"ids of nodes that the tests will run on. Omitting this implies all nodes.")
	cmd.Flags().BoolVar(&onlyDisk, "only-disk-test", false, "Runs only the disk benchmarks")
	cmd.Flags().BoolVar(&onlyNetwork, "only-network-test", false, "Runs only network benchmarks")
	cmd.MarkFlagsMutuallyExclusive("only-disk-test", "only-network-test")
	return cmd
}

// assembleTests creates types of pre-canned tests depending on user input
// 1. onlyDisk - Only runs the throughput/latency disk benchmarks
// 2. onlyNetwork - Only runs the network benchmark
// 3. All false - Runs the default which is the combination of option 1 & 2.
func assembleTests(onlyDisk bool, onlyNetwork bool, durationDisk uint, durationNet uint) []any {
	diskcheck := []any{
		// One test weighted for better throughput results
		admin.DiskcheckParameters{
			Name:        "512K sequential r/w throughput disk test",
			DSync:       true,
			SkipWrite:   false,
			SkipRead:    false,
			DataSize:    1 * units.GiB,
			RequestSize: 512 * units.KiB,
			DurationMs:  durationDisk,
			Parallelism: 4,
			Type:        admin.DiskcheckTagIdentifier,
		},
		// .. and another for better latency/iops results
		admin.DiskcheckParameters{
			Name:        "4k sequential r/w latency/iops disk test",
			DSync:       true,
			SkipWrite:   false,
			SkipRead:    false,
			DataSize:    1 * units.GiB,
			RequestSize: 4 * units.KiB,
			DurationMs:  durationDisk,
			Parallelism: 2,
			Type:        admin.DiskcheckTagIdentifier,
		},
	}
	netcheck := []any{
		admin.NetcheckParameters{
			Name:        "8K Network Throughput Test",
			RequestSize: 8192,
			DurationMs:  durationNet,
			Parallelism: 10,
			Type:        admin.NetcheckTagIdentifier,
		},
	}
	switch {
	case onlyDisk:
		return diskcheck
	case onlyNetwork:
		return netcheck
	default:
		return append(diskcheck, netcheck...)
	}
}
