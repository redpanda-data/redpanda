// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
package connections

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestGetConnectionDuration(t *testing.T) {
	now := time.Now().UTC()
	closed := timestamppb.New(now.Add(-5 * time.Second))
	opened := timestamppb.New(now.Add(-10 * time.Second))

	t.Run("open", func(t *testing.T) {
		require.Equal(t, "10s", getConnectionDuration(opened.AsTime(), time.Time{}))
	})

	t.Run("closed", func(t *testing.T) {
		require.Equal(t, "5s", getConnectionDuration(opened.AsTime(), closed.AsTime()))
	})
}

func TestBuildFilterClauses(t *testing.T) {
	// Make sure we correctly build a set of filters based on the flags we're given
	cases := []struct {
		name     string
		filters  *flagFilters
		expected []string
		err      error
	}{
		{
			name:     "state",
			filters:  &flagFilters{state: "OPEN"},
			expected: []string{"state = KAFKA_CONNECTION_STATE_OPEN"},
		},
		{
			name:    "invalid state",
			filters: &flagFilters{state: "FANCY"},
			err:     errInvalidStateFilter,
		},
		{
			name: "normal options",
			filters: &flagFilters{
				ipAddress: "127.0.0.1",
				clientID:  "clientid", clientSoftwareName: "softwarename",
				clientSoftwareVersion: "softwareversion",
				groupID:               "groupid",
				user:                  "principal",
			},
			expected: []string{
				`source.ip_address = "127.0.0.1"`,
				`client_id = "clientid"`,
				`client_software_name = "softwarename"`,
				`client_software_version = "softwareversion"`,
				`group_id = "groupid"`,
				`authentication_info.user_principal = "principal"`,
			},
		},
		{
			name:     "idle",
			filters:  &flagFilters{idleMs: 2000},
			expected: []string{"idle_duration > 2000ms"},
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			actual, err := tt.filters.buildFilterClauses()
			if tt.err != nil {
				require.ErrorIs(t, err, tt.err, "expected error")
			} else {
				require.ElementsMatch(t, tt.expected, actual)
			}
		})
	}
}

func TestUserDisplay(t *testing.T) {
	t.Run("logged in", func(t *testing.T) {
		output := printConnectionListTable([]*Connection{
			{
				Client:              &Client{},
				RequestStatistics1m: &RequestStatistics{},
				Authentication: &Authentication{
					State:         "SUCCESS",
					UserPrincipal: "theuser",
				},
			},
		})

		require.Contains(t, strings.Split(output, "\n")[1], "theuser")
	})

	t.Run("unauthenticated", func(t *testing.T) {
		output := printConnectionListTable([]*Connection{
			{
				Client:              &Client{},
				RequestStatistics1m: &RequestStatistics{},
				Authentication: &Authentication{
					State: "UNAUTHENTICATED",
				},
			},
		})

		require.Contains(t, strings.Split(output, "\n")[1], "UNAUTHENTICATED")
	})
}

func TestConnectionsList(t *testing.T) {
	t.Run("empty response", func(t *testing.T) {
		display := printConnectionListTable([]*Connection{})
		require.Equal(t, "No open connections found.", display)
	})

	openConn := Connection{
		NodeID:  1,
		ShardID: 2,
		UID:     "uid",
		State:   "OPEN",
		Authentication: &Authentication{
			State:         "SUCCESS",
			Mechanism:     "MTLS",
			UserPrincipal: "principal",
		},
		Client:             &Client{IP: "127.0.0.1", Port: 1234, ID: "client-id", SoftwareName: "client software name", SoftwareVersion: "v0.0.1"},
		Group:              &GroupInfo{ID: "group id"},
		ConnectionDuration: "10s",
		IdleDuration:       "100ms",
		RequestStatisticsAll: &RequestStatistics{
			ProduceBytes:      1_000_000,
			FetchBytes:        900_000,
			RequestCount:      1000,
			ProduceBatchCount: 2000,
		},
		RequestStatistics1m: &RequestStatistics{
			ProduceBytes:      120_000,
			FetchBytes:        60_000,
			RequestCount:      100,
			ProduceBatchCount: 200,
		},
	}

	display := printConnectionListTable([]*Connection{&openConn})

	// Fetch the first line and make sure it has the required headers in it in the right order
	parts := strings.Split(display, "\n")

	require.Equal(t, tableHeaders, strings.Fields(parts[0]))
	require.Equal(t, []string{
		"uid",            // UID
		"OPEN",           // STATE
		"principal",      // USER
		"client-id",      // CLIENT-ID
		"127.0.0.1:1234", // IP:PORT
		"1",              // NODE
		"2",              // SHARD
		"10s",            // OPEN-TIME
		"100ms",          // IDLE
		"2kB",            // PROD-TPUT (120_000B / 60s)
		"1kB",            // FETCH-TPUT (60_000B / 60s)
		"100",            // REQS/MIN
	}, strings.Fields(parts[1]))
}
