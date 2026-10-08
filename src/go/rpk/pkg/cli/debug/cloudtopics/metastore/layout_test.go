// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package metastore

import (
	"bytes"
	"errors"
	"testing"

	cloudtopicsv1 "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/debug/cloudtopics/metastore/internal/cloudtopicsv1"
	"github.com/stretchr/testify/require"
)

func TestFormatBytesIEC(t *testing.T) {
	require.Equal(t, "0 B", formatBytesIEC(0))
	require.Equal(t, "512 B", formatBytesIEC(512))
	require.Equal(t, "1.0 KiB", formatBytesIEC(1024))
	require.Equal(t, "1.5 MiB", formatBytesIEC(1024*1024*3/2))
	require.Equal(t, "1.0 GiB", formatBytesIEC(1024*1024*1024))
}

func TestRenderLayoutTable_BasicTwoPartitions(t *testing.T) {
	results := []partitionResult{
		{
			Partition: 0,
			Stats: &cloudtopicsv1.GetDatabaseStatsResponse{
				ActiveMemtableBytes:    2 * 1024 * 1024,
				ImmutableMemtableBytes: 0,
				TotalSizeBytes:         18 * 1024 * 1024,
				Levels: []*cloudtopicsv1.LsmLevel{
					{LevelNumber: 0, Files: []*cloudtopicsv1.LsmFile{
						{SizeBytes: 2 * 1024 * 1024}, {SizeBytes: 2*1024*1024 + 200000},
					}},
					{LevelNumber: 1, Files: []*cloudtopicsv1.LsmFile{
						{SizeBytes: 4 * 1024 * 1024}, {SizeBytes: 4 * 1024 * 1024}, {SizeBytes: 3 * 1024 * 1024},
					}},
				},
			},
		},
		{
			Partition: 1,
			Stats: &cloudtopicsv1.GetDatabaseStatsResponse{
				ActiveMemtableBytes:    3 * 1024 * 1024,
				ImmutableMemtableBytes: 1024 * 1024,
				TotalSizeBytes:         22 * 1024 * 1024,
				Levels: []*cloudtopicsv1.LsmLevel{
					{LevelNumber: 0, Files: []*cloudtopicsv1.LsmFile{{SizeBytes: 2 * 1024 * 1024}}},
					{LevelNumber: 2, Files: []*cloudtopicsv1.LsmFile{{SizeBytes: 4*1024*1024 + 512*1024}}},
				},
			},
		},
	}

	var buf bytes.Buffer
	renderLayoutTable(&buf, results)
	got := buf.String()

	require.Contains(t, got, "PART")
	require.Contains(t, got, "MEMTABLE")
	require.Contains(t, got, "IMM")
	require.Contains(t, got, "TOTAL")
	require.Contains(t, got, "L0#")
	require.Contains(t, got, "L1#")
	require.Contains(t, got, "L2#") // deepest level present anywhere

	// Spot-check rendered data values rather than full column widths.
	require.Contains(t, got, "2.0 MiB") // partition 0 active memtable
	require.Contains(t, got, "18 MiB")  // partition 0 total — rounded to whole MiB
	require.Contains(t, got, "22 MiB")  // partition 1 total
}

func TestRenderLayoutTable_ErrorRow(t *testing.T) {
	results := []partitionResult{
		{Partition: 0, Err: errors.New("boom")},
		{Partition: 1, Stats: &cloudtopicsv1.GetDatabaseStatsResponse{TotalSizeBytes: 1024}},
	}
	var buf bytes.Buffer
	renderLayoutTable(&buf, results)
	got := buf.String()
	require.Contains(t, got, "0")
	require.Contains(t, got, "ERROR")
	require.Contains(t, got, "partition 0: boom") // footer
	require.Contains(t, got, "1")
	require.Contains(t, got, "1.0 KiB")
}

func TestMaxLevel(t *testing.T) {
	require.Equal(t, int32(-1), maxLevel(nil))
	results := []partitionResult{
		{Stats: &cloudtopicsv1.GetDatabaseStatsResponse{Levels: []*cloudtopicsv1.LsmLevel{{LevelNumber: 0}, {LevelNumber: 2}}}},
		{Stats: &cloudtopicsv1.GetDatabaseStatsResponse{Levels: []*cloudtopicsv1.LsmLevel{{LevelNumber: 1}}}},
		{Err: errors.New("err — should be skipped")},
	}
	require.Equal(t, int32(2), maxLevel(results))
}
