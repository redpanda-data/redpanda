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
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/redpanda-data/common-go/rpadmin"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/system"

	"github.com/docker/go-units"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

const (
	statusIdle    = "idle"
	statusRunning = "running"
)

func newStatusCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var format string
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Returns the status of the current running tests or the cached results of the last completed run.",
		Long: `Returns the status of the current running tests or the cached results of the last completed run.

Use this command after invoking 'self-test start' to determine the status of
the jobs launched. Possible results are:

* One or more jobs still running
  * Returns the IDs of Redpanda brokers (nodes) still running self-tests.

* No jobs running:
  * Returns the cached results for all brokers of the last completed test.

Test results are grouped by broker ID. Each test returns the following:

* Name: Description of the test.
* Info: Details about the test run attached by Redpanda.
* Type: Either 'disk', 'network', or 'cloud' test.
* Test Id: Unique identifier given to jobs of a run. All IDs in a test should match. If they don't match, then newer and/or older test results have been included erroneously.
* Timeouts: Number of timeouts incurred during the test.
* Start time: Time that the test started, in UTC.
* End time: Time that the test ended, in UTC.
* Avg Duration: Duration of the test.
* IOPS: Number of operations per second. For disk, it's 'seastar::dma_read' and 'seastar::dma_write'. For network, it's 'rpc.send()'.
* Throughput: For disk, throughput rate is in bytes per second. For network, throughput rate is in bits per second. Note that GiB vs. Gib is the correct notation displayed by the UI.
* Latency: 50th, 90th, etc. percentiles of operation latency, reported in microseconds (μs). Represented as P50, P90, P99, P999, and MAX respectively.
If Tiered Storage is not enabled, the cloud storage tests won't run and a warning will be displayed showing "Cloud storage is not enabled.". All results will be shown as 0.
`,
		Args: cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, _ []string) {
			// Load config settings
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			config.CheckExitCloudAdmin(p)

			// Create new HTTP client for communication w/ admin server
			cl, err := adminapi.NewClient(cmd.Context(), fs, p)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			// Make HTTP GET request to any node requesting for status
			// Returns last runs results, or status of which nodes have jobs running
			reports, err := cl.SelfTestStatus(cmd.Context())
			out.MaybeDie(err, "unable to query self-test status: %v", err)

			if format == "json" {
				asJSON, err := json.MarshalIndent(reports, "", "\t")
				out.MaybeDie(err, "unable to format response as JSON: %v", err)
				fmt.Print(string(asJSON))
				return
			}

			// If there is outstanding work, indicate which nodes, then exit
			running := runningNodes(reports)
			if len(running) > 0 {
				keys := make([]int, 0, len(running))
				for k := range running {
					keys = append(keys, k)
				}
				sort.Ints(keys)
				for _, k := range keys {
					fmt.Printf("Node %v is still running %v self test\n", k, running[k])
				}
				return
			}

			// .. or redpanda has never run any tests, no cached data exists
			if isUninitialized(reports) {
				fmt.Println("All nodes are idle with no cached test results")
				return
			}

			// In all other cases there are results, print them and exit
			tw := out.NewTabWriter()
			defer tw.Flush()
			for _, report := range reports {
				header := makeReportHeader(report)
				tw.PrintColumn(header)
				tw.PrintColumn(strings.Repeat("=", len(header)))
				tableResults := makeReportTable(report)
				if len(tableResults) == 0 {
					tw.PrintColumn("INFO", "No cached results for node")
					tw.Line()
					continue
				}
				for _, row := range tableResults {
					all := rowDataAsInterface(row[1:])
					tw.PrintColumn(row[0], all...)
				}
			}
		},
	}
	cmd.Flags().StringVar(&format, "format", "text", "Output format (text, json)")
	return cmd
}

func rowDataAsInterface(row []string) []interface{} {
	var iarr []interface{}
	for _, x := range row {
		iarr = append(iarr, x)
	}
	return iarr
}

func runningNodes(reports []rpadmin.SelfTestNodeReport) map[int]string {
	running := map[int]string{}
	for _, report := range reports {
		if report.Status == statusRunning {
			running[report.NodeID] = report.Stage
		}
	}
	return running
}

func isUninitialized(reports []rpadmin.SelfTestNodeReport) bool {
	noResults := 0
	for _, report := range reports {
		if report.Status == statusIdle && len(report.Results) == 0 {
			noResults += 1
		}
	}
	return noResults == len(reports)
}

func makeReportHeader(report rpadmin.SelfTestNodeReport) string {
	return fmt.Sprintf("NODE ID: %d | STATUS: %s", report.NodeID, report.Status)
}

func makeReportTable(report rpadmin.SelfTestNodeReport) [][]string {
	var table [][]string
	for _, sr := range report.Results {
		table = append(table, []string{"NAME", sr.TestName})
		if sr.TestInfo != "" {
			table = append(table, []string{"INFO", sr.TestInfo})
		}
		table = append(table, []string{"TYPE", sr.TestType})
		table = append(table, []string{"TEST ID", sr.TestID})
		table = append(table, []string{"TIMEOUTS", fmt.Sprintf("%d", sr.Timeouts)})
		table = append(table, []string{"START TIME", time.Unix(sr.StartTime, 0).UTC().Format(time.UnixDate)})
		table = append(table, []string{"END TIME", time.Unix(sr.EndTime, 0).UTC().Format(time.UnixDate)})
		table = append(table, []string{"AVG DURATION", fmt.Sprintf("%dms", sr.Duration)})
		if sr.Warning != nil {
			table = append(table, []string{"WARNING", *sr.Warning})
		}
		if sr.Error != nil {
			table = append(table, []string{"ERROR", *sr.Error})
			table = append(table, []string{""})
			continue
		}
		if sr.TestType == rpadmin.CloudcheckTagIdentifier {
			// Cloudcheck does not have any IOPS, THROUGHPUT, or LATENCY results.
			table = append(table, []string{""})
			continue
		}
		table = append(table, []string{"IOPS", fmt.Sprintf("%d req/sec", *sr.RequestsPerSec)})
		var throughput string
		if sr.TestType == rpadmin.NetcheckTagIdentifier {
			throughput = system.BitsToHuman(float64(*sr.BytesPerSec))
		} else {
			throughput = units.BytesSize(float64(*sr.BytesPerSec))
		}
		table = append(table, []string{"THROUGHPUT", fmt.Sprintf("%s/sec", throughput)})
		table = append(table, []string{"LATENCY", "P50", "P90", "P99", "P999", "MAX"})
		table = append(table, []string{
			"",
			fmt.Sprintf("%dus", *sr.P50),
			fmt.Sprintf("%dus", *sr.P90),
			fmt.Sprintf("%dus", *sr.P99),
			fmt.Sprintf("%dus", *sr.P999),
			fmt.Sprintf("%dus", *sr.MaxLatency),
		})
		table = append(table, []string{""})
	}
	return table
}
