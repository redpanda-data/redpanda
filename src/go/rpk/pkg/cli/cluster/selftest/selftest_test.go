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
	"testing"

	"github.com/redpanda-data/common-go/rpadmin"

	"github.com/stretchr/testify/require"
)

func TestClusterStatus(t *testing.T) {
	for _, test := range []struct {
		name            string
		serverResponse  string
		runningNodes    map[int]string
		isUninitialized bool
	}{
		{
			name: "runningNodes all running case",
			serverResponse: `[
               {
                 "node_id": 1,
                 "status": "running",
                 "stage": "disk"
               },
               {
                 "node_id": 0,
                 "status": "running",
                 "stage": "cloud"
               },
               {
                 "node_id": 2,
                 "status": "running",
                 "stage": "net"
               }
            ]`,
			runningNodes:    map[int]string{0: "cloud", 1: "disk", 2: "net"},
			isUninitialized: false,
		},
		{
			name: "runningNodes some running case",
			serverResponse: `[
               {
                 "node_id": 1,
                 "status": "running",
                 "stage": "disk"
               },
               {
                 "node_id": 0,
                 "status": "idle",
                 "stage": "idle"
               },
               {
                 "node_id": 2,
                 "status": "idle",
                 "stage": "idle"
               }
            ]`,
			runningNodes:    map[int]string{1: "disk"},
			isUninitialized: false,
		},
		{
			name: "runningNodes method negative case",
			serverResponse: `[
               {
                 "node_id": 1,
                 "status": "idle",
                 "stage": "idle"
               },
               {
                 "node_id": 0,
                 "status": "idle",
                 "stage": "idle"
               },
               {
                 "node_id": 2,
                 "status": "idle",
                 "stage": "idle"
               }
            ]`,
			runningNodes:    map[int]string{},
			isUninitialized: true,
		},
		{
			name: "isUninitialized some init'ed condition",
			serverResponse: `[
               {
                 "node_id": 1,
                 "status": "idle",
                 "stage": "idle",
                 "results": [{}]
               },
               {
                 "node_id": 0,
                 "status": "idle",
                 "stage": "idle"
               },
               {
                 "node_id": 2,
                 "status": "idle",
                 "stage": "idle"
               }
            ]`,
			runningNodes:    map[int]string{},
			isUninitialized: false,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			var reports []rpadmin.SelfTestNodeReport
			json.Unmarshal([]byte(test.serverResponse), &reports)
			running := runningNodes(reports)
			uninited := isUninitialized(reports)
			require.Equal(t, test.runningNodes, running)
			require.Equal(t, test.isUninitialized, uninited)
		})
	}
}

func TestSelfTestResults(t *testing.T) {
	for _, test := range []struct {
		name             string
		serverResponse   string
		expectedHeadings []string
		expectedRows     map[int][][]string
	}{
		{
			name: "makeReportTable() method w/ mixed report types",
			serverResponse: `[
               {
                 "node_id": 1,
                 "status": "idle",
                 "results": [
                   {
                     "p50": 123,
                     "p90": 456,
                     "p99": 789,
                     "p999": 999,
                     "max_latency": 1200,
                     "rps": 2222,
                     "bps": 929283,
                     "timeouts": 1,
                     "test_id": "8272-3843-38c8-381f",
                     "name": "unittesting",
                     "info": "golang unit tests",
                     "test_type": "disk",
                     "start_time": 1717615532,
                     "end_time": 1717615693,
                     "duration": 50000,
                     "warning": "Mild transient issue detected"
                   }
                 ]
               },
               {
                 "node_id": 0,
                 "status": "idle",
                 "results": [
                   {
                     "timeouts": 55,
                     "test_id": "8272-3843-38c8-381f",
                     "name": "unittesting",
                     "info": "golang unit tests",
                     "test_type": "disk",
                     "start_time": 1717615532,
                     "end_time": 1717615693,
                     "duration": 50000,
                     "error": "Unexpected exception detected"
                   }
                 ]
               },
               {
                 "node_id": 2,
                 "status": "idle",
                 "results": [
                   {
                     "timeouts": 78,
                     "test_id": "8272-3843-38c8-381f",
                     "name": "unittesting",
                     "info": "golang unit tests",
                     "test_type": "disk",
                     "start_time": 1717615532,
                     "end_time": 1717615693,
                     "duration": 50000,
                     "error": "Unexpected exception detected"
                   }
                 ]
               }
            ]`,
			expectedHeadings: []string{
				"NODE ID: 0 | STATUS: idle",
				"NODE ID: 1 | STATUS: idle",
				"NODE ID: 2 | STATUS: idle",
			},
			expectedRows: map[int][][]string{
				1: {
					{"NAME", "unittesting"},
					{"INFO", "golang unit tests"},
					{"TYPE", "disk"},
					{"TEST ID", "8272-3843-38c8-381f"},
					{"TIMEOUTS", "1"},
					{"START TIME", "Wed Jun  5 19:25:32 UTC 2024"},
					{"END TIME", "Wed Jun  5 19:28:13 UTC 2024"},
					{"AVG DURATION", "50000ms"},
					{"IOPS", "2222 req/sec"},
					{"THROUGHPUT", "907.5KiB/sec"},
					{"WARNING", "Mild transient issue detected"},
					{"LATENCY", "P50", "P90", "P99", "P999", "MAX"},
					{"", "123us", "456us", "789us", "999us", "1200us"},
					{""},
				},
				0: {
					[]string{"NAME", "unittesting"},
					[]string{"INFO", "golang unit tests"},
					[]string{"TYPE", "disk"},
					[]string{"TEST ID", "8272-3843-38c8-381f"},
					[]string{"TIMEOUTS", "55"},
					[]string{"START TIME", "Wed Jun  5 19:25:32 UTC 2024"},
					[]string{"END TIME", "Wed Jun  5 19:28:13 UTC 2024"},
					[]string{"AVG DURATION", "50000ms"},
					[]string{"ERROR", "Unexpected exception detected"},
					[]string{""},
				},
				2: {
					[]string{"NAME", "unittesting"},
					[]string{"INFO", "golang unit tests"},
					[]string{"TYPE", "disk"},
					[]string{"TEST ID", "8272-3843-38c8-381f"},
					[]string{"TIMEOUTS", "78"},
					[]string{"START TIME", "Wed Jun  5 19:25:32 UTC 2024"},
					[]string{"END TIME", "Wed Jun  5 19:28:13 UTC 2024"},
					[]string{"AVG DURATION", "50000ms"},
					[]string{"ERROR", "Unexpected exception detected"},
					[]string{""},
				},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			var reports []rpadmin.SelfTestNodeReport
			json.Unmarshal([]byte(test.serverResponse), &reports)
			require.Equal(t, len(reports), len(test.expectedHeadings))
			for _, report := range reports {
				header := makeReportHeader(report)
				require.Contains(t, test.expectedHeadings, header)
				tableResults := makeReportTable(report)
				expReport := test.expectedRows[report.NodeID]
				totalRows := 0
				for _, row := range tableResults {
					require.Contains(t, expReport, row)
					totalRows += 1
				}
				require.Equal(t, len(expReport), totalRows)
			}
		})
	}
}
