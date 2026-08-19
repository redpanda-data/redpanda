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
	"context"
	"fmt"
	"io"
	"time"

	"github.com/redpanda-data/common-go/rpadmin"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"

	"github.com/docker/go-units"

	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

// selfTestWatchInterval is how often 'self-test start --watch' polls the
// cluster for self-test status updates.
const selfTestWatchInterval = 2 * time.Second

func newStartCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var (
		noConfirm      bool
		diskDurationMs uint
		netDurationMs  uint
		cloudTimeoutMs uint
		cloudBackoffMs uint
		onNodes        []int
		onlyDisk       bool
		onlyNetwork    bool
		onlyCloud      bool
		watch          bool
	)
	cmd := &cobra.Command{
		Use:   "start",
		Short: "Starts a new self-test run",
		Long: `Starts one or more benchmark tests on one or more nodes of the cluster.

NOTE: Redpanda self-test runs benchmarks that consume significant system resources. Do not start self-test if large workloads are already running on the system.

Available tests to run:

* Disk tests:
  ** Throughput test: 512 KB messages, sequential read/write
     *** Uses a larger request message sizes and deeper I/O queue depth to write/read more bytes in a shorter amount of time, at the cost of IOPS/latency.
  ** Latency and io depth tests: 4 KB messages, sequential read/write, varying io depth
     *** Uses small IO sizes and varying levels of parallelism to determine the relationship between io depth and IOPS
     *** Includes one test without using dsync (fdatasync) on each write to establish the cost of dsync
  ** 16 KB test
     *** One high io depth test at 16 KB to reflect performance at Redpanda's default chunk size
* Network tests:
  ** Throughput test: 8192-bit messages
     *** Unique pairs of Redpanda nodes each act as a client and a server.
     *** The test pushes as much data over the wire, within the test parameters.
* Cloud storage tests
  ** Configuration/Latency test: 1024-byte object.
  ** If cloud storage is enabled ('cloud_storage_enabled'), a series of remote operations are performed:
     *** Upload an object (a random buffer of 1024 bytes) to the cloud storage bucket/container.
     *** List objects in the bucket/container.
     *** Download the uploaded object from the bucket/container.
     *** Download the uploaded object's metadata from the bucket/container.
     *** Delete the uploaded object from the bucket/container.
     *** Upload and then delete multiple objects (random buffers of 1024 bytes) at once from the bucket/container.

This command prompts users for confirmation (unless the flag '--no-confirm' is specified), then returns a test identifier ID, and runs the tests.

To view the test status, poll 'rpk cluster self-test status'. Once the tests end, the cached results will be available with 'rpk cluster self-test status'.

Pass '--watch' to poll the status automatically and wait until the tests complete.`,
		Args: cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, _ []string) {
			// Load config settings
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			config.CheckExitCloudAdmin(p)

			// Warn user before continuing, proceed only via explicit signal
			if !noConfirm {
				confirmed, err := out.Confirm("Redpanda self-test will run benchmarks of disk and network hardware that will consume significant system resources. Do not start self-test if large workloads are already running on the system.")
				out.MaybeDie(err, "unable to confirm user confirmation: %v", err)
				if !confirmed {
					out.Exit("self-test start was cancelled")
				}
			}

			// Create new HTTP client for communication w/ admin server
			cl, err := adminapi.NewClient(cmd.Context(), fs, p)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			// Using cmd line args, assemble self_test_start_request body
			tests := assembleTests(onlyDisk, onlyNetwork, onlyCloud, diskDurationMs, netDurationMs, cloudTimeoutMs, cloudBackoffMs)

			// Make HTTP POST request to leader that starts the actual test
			tid, err := cl.StartSelfTest(cmd.Context(), onNodes, tests)
			out.MaybeDie(err, "unable to start self test: %v", err)

			// With --watch we poll the status until the tests finish
			// instead of asking the user to poll manually.
			if watch {
				fmt.Printf("Redpanda self-test has started, test identifier: %v\n", tid)
				err = watchSelfTest(cmd.Context(), cl, config.OutFormatter{Kind: "text"}, cmd.OutOrStdout(), selfTestWatchInterval)
				out.MaybeDieErr(err)
				return
			}
			fmt.Printf("Redpanda self-test has started, test identifier: %v, To check the status run:\n    rpk cluster self-test status\n", tid)
		},
	}

	// Collect arguments via command line
	cmd.Flags().BoolVar(&noConfirm, "no-confirm", false, "Acknowledge warning prompt skipping read from stdin")
	cmd.Flags().UintVar(&diskDurationMs, "disk-duration-ms", 30000,
		"The duration in milliseconds of individual disk test runs")
	cmd.Flags().UintVar(&netDurationMs, "network-duration-ms", 30000,
		"The duration in milliseconds of individual network test runs")
	cmd.Flags().UintVar(&cloudTimeoutMs, "cloud-timeout-ms", 10000,
		"The timeout in milliseconds for a cloud storage request")
	cmd.Flags().UintVar(&cloudBackoffMs, "cloud-backoff-ms", 100,
		"The backoff in milliseconds for a cloud storage request")
	cmd.Flags().IntSliceVar(&onNodes, "participant-node-ids", nil,
		"Comma-separated list of broker IDs that the tests will run on. If not set, tests will run for all node IDs.")
	cmd.Flags().BoolVar(&onlyDisk, "only-disk-test", false, "Runs only the disk benchmarks")
	cmd.Flags().BoolVar(&onlyNetwork, "only-network-test", false, "Runs only network benchmarks")
	cmd.Flags().BoolVar(&onlyCloud, "only-cloud-test", false, "Runs only cloud storage verification")
	cmd.Flags().BoolVarP(&watch, "watch", "w", false, "Poll the self-test status and wait until the tests complete")
	cmd.MarkFlagsMutuallyExclusive("only-disk-test", "only-network-test", "only-cloud-test")
	return cmd
}

// selfTestStatusClient is the subset of the admin client used to poll
// self-test status. It exists so watchSelfTest can be tested without a live
// cluster.
type selfTestStatusClient interface {
	SelfTestStatus(context.Context) ([]rpadmin.SelfTestNodeReport, error)
}

// watchSelfTest polls the self-test status every interval, showing a spinner
// with elapsed time while a test is still running. Once every node is idle it
// stops the spinner and prints the final status. It backs the --watch flag of
// 'rpk cluster self-test start'.
func watchSelfTest(ctx context.Context, cl selfTestStatusClient, f config.OutFormatter, w io.Writer, interval time.Duration) error {
	s := out.NewSpinner(ctx, "Running self-test", out.WithOutput(w), out.WithElapsedTime())
	for {
		reports, err := cl.SelfTestStatus(ctx)
		if err != nil {
			s.Fail("Self-test status query failed")
			return fmt.Errorf("unable to query self-test status: %w", err)
		}
		if len(runningNodes(reports)) == 0 {
			s.Success("Self-test complete")
			return printSelfTestStatus(f, reports, w)
		}
		select {
		case <-ctx.Done():
			s.Stop()
			return ctx.Err()
		case <-time.After(interval):
		}
	}
}

// assembleTests creates types of pre-canned tests depending on user input
// 1. onlyDisk - Only runs the throughput/latency disk benchmarks
// 2. onlyNetwork - Only runs the network benchmark
// 3. onlyCloud - Only runs the cloud storage verification
// 4. All false - Runs the default which is the combination of all tests.
func assembleTests(onlyDisk bool, onlyNetwork bool, onlyCloud bool, durationDisk uint, durationNet uint, timeoutCloud uint, backoffCloud uint) []any {
	diskcheck := []any{
		// One test weighted for better throughput results
		rpadmin.DiskcheckParameters{
			Name:        "512KB sequential r/w",
			DSync:       true,
			SkipWrite:   false,
			SkipRead:    false,
			DataSize:    1 * units.GiB,
			RequestSize: 512 * units.KiB,
			DurationMs:  durationDisk,
			Parallelism: 4,
			Type:        rpadmin.DiskcheckTagIdentifier,
		},
		// .. and then a series of 4KB write-only tests at increasing io depth
		rpadmin.DiskcheckParameters{
			Name:        "4KB sequential r/w, low io depth",
			DSync:       true,
			SkipWrite:   false,
			SkipRead:    false,
			DataSize:    1 * units.GiB,
			RequestSize: 4 * units.KiB,
			DurationMs:  durationDisk,
			Parallelism: 1,
			Type:        rpadmin.DiskcheckTagIdentifier,
		},
		rpadmin.DiskcheckParameters{
			Name:        "4KB sequential write, medium io depth",
			DSync:       true,
			SkipWrite:   false,
			SkipRead:    true,
			DataSize:    1 * units.GiB,
			RequestSize: 4 * units.KiB,
			DurationMs:  durationDisk,
			Parallelism: 8,
			Type:        rpadmin.DiskcheckTagIdentifier,
		},
		rpadmin.DiskcheckParameters{
			Name:        "4KB sequential write, high io depth",
			DSync:       true,
			SkipWrite:   false,
			SkipRead:    true,
			DataSize:    1 * units.GiB,
			RequestSize: 4 * units.KiB,
			DurationMs:  durationDisk,
			Parallelism: 64,
			Type:        rpadmin.DiskcheckTagIdentifier,
		},
		rpadmin.DiskcheckParameters{
			Name:        "4KB sequential write, very high io depth",
			DSync:       true,
			SkipWrite:   false,
			SkipRead:    true,
			DataSize:    1 * units.GiB,
			RequestSize: 4 * units.KiB,
			DurationMs:  durationDisk,
			Parallelism: 256,
			Type:        rpadmin.DiskcheckTagIdentifier,
		},
		// ... and a 4KB test as above but with dsync off
		rpadmin.DiskcheckParameters{
			Name:        "4KB sequential write, no dsync",
			DSync:       false,
			SkipWrite:   false,
			SkipRead:    true,
			DataSize:    1 * units.GiB,
			RequestSize: 4 * units.KiB,
			DurationMs:  durationDisk,
			Parallelism: 64,
			Type:        rpadmin.DiskcheckTagIdentifier,
		},
		// ... and a 16KB test as above as another important size for redpanda
		rpadmin.DiskcheckParameters{
			Name:        "16KB sequential r/w, high io depth",
			DSync:       false,
			SkipWrite:   false,
			SkipRead:    false,
			DataSize:    1 * units.GiB,
			RequestSize: 16 * units.KiB,
			DurationMs:  durationDisk,
			Parallelism: 64,
			Type:        rpadmin.DiskcheckTagIdentifier,
		},
	}
	netcheck := []any{
		rpadmin.NetcheckParameters{
			Name:        "8Kb Network Throughput Test",
			RequestSize: 8192,
			DurationMs:  durationNet,
			Parallelism: 10,
			Type:        rpadmin.NetcheckTagIdentifier,
		},
	}
	cloudcheck := []any{
		rpadmin.CloudcheckParameters{
			Name:      "Cloud Storage Test",
			TimeoutMs: timeoutCloud,
			BackoffMs: backoffCloud,
			Type:      rpadmin.CloudcheckTagIdentifier,
		},
	}
	switch {
	case onlyDisk:
		return diskcheck
	case onlyNetwork:
		return netcheck
	case onlyCloud:
		return cloudcheck
	default:
		return append(append(diskcheck, netcheck...), cloudcheck...)
	}
}
