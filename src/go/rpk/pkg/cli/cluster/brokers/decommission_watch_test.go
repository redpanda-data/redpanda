// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package brokers

import (
	"testing"

	"github.com/redpanda-data/common-go/rpadmin"
	"github.com/stretchr/testify/require"
)

func TestComputeDecommissionProgress(t *testing.T) {
	for _, tc := range []struct {
		name        string
		dbs         rpadmin.DecommissionStatusResponse
		wantRemain  int
		wantMoved   int
		wantLeft    int
		wantPercent int
	}{
		{
			name:        "nothing measurable reports zero, not done",
			dbs:         rpadmin.DecommissionStatusResponse{},
			wantPercent: 0,
		},
		{
			name: "partitions listed but no sizes reported yet",
			dbs: rpadmin.DecommissionStatusResponse{
				Partitions: []rpadmin.DecommissionPartitions{{}, {}},
			},
			wantRemain:  2,
			wantPercent: 0,
		},
		{
			name: "partial progress",
			dbs: rpadmin.DecommissionStatusResponse{
				Partitions: []rpadmin.DecommissionPartitions{
					{BytesMoved: 30, BytesLeftToMove: 70},
					{BytesMoved: 10, BytesLeftToMove: 90},
				},
			},
			wantRemain:  2,
			wantMoved:   40,
			wantLeft:    160,
			wantPercent: 20,
		},
		{
			name: "all bytes moved but partitions still listed",
			dbs: rpadmin.DecommissionStatusResponse{
				Partitions: []rpadmin.DecommissionPartitions{
					{BytesMoved: 100, BytesLeftToMove: 0},
				},
			},
			wantRemain:  1,
			wantMoved:   100,
			wantPercent: 100,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := computeDecommissionProgress(tc.dbs)
			require.Equal(t, tc.wantRemain, p.partitionsMoving)
			require.Equal(t, tc.wantMoved, p.bytesMoved)
			require.Equal(t, tc.wantLeft, p.bytesRemaining)
			require.Equal(t, tc.wantPercent, p.percent())
		})
	}
}

func TestDecommissionProgressLine(t *testing.T) {
	dbs := rpadmin.DecommissionStatusResponse{
		Partitions: []rpadmin.DecommissionPartitions{
			{BytesMoved: 42, BytesLeftToMove: 58},
		},
	}
	require.Equal(t,
		"broker 3: 42% complete (1 partitions remaining, 58 left to move)",
		decommissionProgressLine(3, dbs, false),
	)

	// Human-readable sizing uses units.HumanSize, which formats in SI units
	// (MB, not MiB).
	human := rpadmin.DecommissionStatusResponse{
		Partitions: []rpadmin.DecommissionPartitions{
			{BytesMoved: 0, BytesLeftToMove: 2000000},
		},
	}
	require.Contains(t, decommissionProgressLine(4, human, true), "2MB left to move")

	// With no partitions in the moving set yet, the line reports that we are
	// waiting for the cluster to schedule movement.
	require.Equal(t,
		"broker 5: waiting for the cluster to schedule partition movement",
		decommissionProgressLine(5, rpadmin.DecommissionStatusResponse{}, false),
	)
}

func TestFailureSuffix(t *testing.T) {
	require.Empty(t, failureSuffix(rpadmin.DecommissionStatusResponse{}))

	// Reallocation failures take precedence and are pluralized.
	realloc := rpadmin.DecommissionStatusResponse{
		ReallocationFailureDetails: []rpadmin.ReallocationFailedPartition{{}, {}},
		AllocationFailures:         []string{"kafka/foo/0"},
	}
	require.Equal(t, ", 2 reallocation failures (retrying)", failureSuffix(realloc))

	alloc := rpadmin.DecommissionStatusResponse{
		AllocationFailures: []string{"kafka/foo/0"},
	}
	require.Equal(t, ", 1 allocation failure (retrying)", failureSuffix(alloc))
}
