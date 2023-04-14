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

func newStartCommand(fs afero.Fs, p *config.Params) *cobra.Command {
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
		Short: "Starts a new self-test run",
		Long: `Starts one or more benchmark tests on one or more nodes
of the cluster. Available tests to run:

* Disk tests:
  * Throughput test: 512 KB messages, sequential read/write
    * Uses a larger request message sizes and deeper I/O queue depth to write/read more bytes in a shorter amount of time, at the cost of IOPS/latency.
  * Latency test: 4 KB messages, sequential read/write
    * Uses smaller request message sizes and lower levels of parallelism to achieve higher IOPS and lower latency.

* Network tests:
  * Throughput test: 8192-bit messages
    * Unique pairs of Redpanda nodes each act as a client and a server.
    * The test pushes as much data over the wire, within the test parameters.

This command immediately returns on success, and the tests run asynchronously. The
user polls for results with the 'self-test status'
command.`,
		Args: cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, _ []string) {
			// Load config settings
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			// Warn user before continuing, proceed only via explicit signal
			if !noConfirm {
				confirmed, err := out.Confirm("Redpanda self-test will run benchmarks of disk and network hardware that will consume significant system resources. Do not start self-test if large workloads are already running on the system.")
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
			fmt.Printf("Redpanda self-test has started, test identifier: %v, To check the status run:\n    rpk cluster self-test status\n", tid)
		},
	}

	// Collect arguments via command line
	cmd.Flags().BoolVar(&noConfirm, "no-confirm", false, "Acknowledge warning prompt skipping read from stdin")
	cmd.Flags().UintVar(&diskDurationMs, "disk-duration-ms", 30000,
		"The duration in milliseconds of individual disk test runs")
	cmd.Flags().UintVar(&netDurationMs, "network-duration-ms", 30000,
		"The duration in milliseconds of individual network test runs")
	cmd.Flags().IntSliceVar(&onNodes, "participant-node-ids", nil,
		"IDs of nodes that the tests will run on. If not set, tests will run for all node IDs.")
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
			Name:        "512KB sequential r/w throughput disk test",
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
			Name:        "4KB sequential r/w latency/iops disk test",
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
			Name:        "8Kb Network Throughput Test",
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
