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
	"strings"
	"testing"

	cloudtopicsv1 "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/debug/cloudtopics/metastore/internal/cloudtopicsv1"
	"github.com/stretchr/testify/require"
)

func mkStats(level int32, files ...*cloudtopicsv1.LsmFile) *cloudtopicsv1.LsmLevel {
	return &cloudtopicsv1.LsmLevel{LevelNumber: level, Files: files}
}

func TestRenderFilesTable_SortAndFilter(t *testing.T) {
	results := []partitionResult{
		{
			Partition: 0,
			Stats: &cloudtopicsv1.GetDatabaseStatsResponse{
				Levels: []*cloudtopicsv1.LsmLevel{
					mkStats(0,
						&cloudtopicsv1.LsmFile{Epoch: 11, Id: 1041, SizeBytes: 2 * 1024 * 1024, SmallestKeyInfo: "meta/def/0/0", LargestKeyInfo: "meta/def/0/9"},
						&cloudtopicsv1.LsmFile{Epoch: 12, Id: 1042, SizeBytes: 2 * 1024 * 1024, SmallestKeyInfo: "meta/abc/0/0", LargestKeyInfo: "meta/abc/0/9"},
					),
					mkStats(1,
						&cloudtopicsv1.LsmFile{Epoch: 11, Id: 987, SizeBytes: 3*1024*1024 + 512*1024, SmallestKeyInfo: "ext/abc/0/0", LargestKeyInfo: "ext/abc/0/4"},
					),
				},
			},
		},
		{
			Partition: 1,
			Stats: &cloudtopicsv1.GetDatabaseStatsResponse{
				Levels: []*cloudtopicsv1.LsmLevel{
					mkStats(0,
						&cloudtopicsv1.LsmFile{Epoch: 12, Id: 1051, SizeBytes: 2 * 1024 * 1024, SmallestKeyInfo: "meta/ghi/0/0", LargestKeyInfo: "meta/ghi/0/9"},
					),
				},
			},
		},
	}

	t.Run("all levels", func(t *testing.T) {
		var buf bytes.Buffer
		renderFilesTable(&buf, results, nil)
		got := buf.String()
		require.Contains(t, got, "PART")
		require.Contains(t, got, "LEVEL")
		require.Contains(t, got, "L0")
		require.Contains(t, got, "L1")
		// Within partition 0 / level 0, epoch 12 should sort before epoch 11.
		i12 := strings.Index(got, "1042")
		i11 := strings.Index(got, "1041")
		require.True(t, i12 >= 0 && i11 >= 0 && i12 < i11, "expected epoch 12 row before epoch 11 row, got %q", got)
	})

	t.Run("level filter", func(t *testing.T) {
		var buf bytes.Buffer
		renderFilesTable(&buf, results, map[int32]bool{1: true})
		got := buf.String()
		require.NotContains(t, got, "L0")
		require.Contains(t, got, "L1")
		require.NotContains(t, got, "1041")
		require.Contains(t, got, "987")
	})
}

func TestRenderFilesTable_ErrorFooter(t *testing.T) {
	results := []partitionResult{
		{Partition: 0, Err: nil, Stats: &cloudtopicsv1.GetDatabaseStatsResponse{}},
		{Partition: 1, Err: testErr("net fail")},
	}
	var buf bytes.Buffer
	renderFilesTable(&buf, results, nil)
	got := buf.String()
	require.Contains(t, got, "partition 1: net fail")
}

type testErr string

func (e testErr) Error() string { return string(e) }
