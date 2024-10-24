package topic

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestDescribePartitions(t *testing.T) {
	// inputs: conditionals for what columns, as well as the rows
	// test: ensure which headers are returned, and args

	for _, test := range []struct {
		name string

		inMeta    []kmsg.MetadataResponseTopicPartition
		inOffsets []startStableEndOffset

		expHeaders []string
		expRows    [][]interface{}
	}{
		{
			name: "all ok, no optional columns, one partition",

			inMeta: []kmsg.MetadataResponseTopicPartition{
				{
					Partition:   0,
					Leader:      0,
					ErrorCode:   0,
					LeaderEpoch: -1,
					Replicas:    []int32{0, 1, 2},
				},
			},
			inOffsets: []startStableEndOffset{{
				start:  0,
				stable: 1,
				end:    1,
			}},

			expHeaders: []string{
				"partition",
				"leader",
				"epoch",
				"replicas",
				"log-start-offset",
				"high-watermark",
			},
			expRows: [][]interface{}{
				{int32(0), int32(0), int32(-1), []int32{0, 1, 2}, int64(0), int64(1)},
			},
		},

		{
			name: "all ok, all extra columns, out of order partitions, errors",

			inMeta: []kmsg.MetadataResponseTopicPartition{
				{
					Partition:       1,
					Leader:          0,
					ErrorCode:       1,
					LeaderEpoch:     0, // optional, used
					Replicas:        []int32{0, 1},
					OfflineReplicas: []int32{2, 3}, // optional, used
				},

				{
					Partition:   0,
					Leader:      1,
					LeaderEpoch: -1,
					Replicas:    []int32{0},
				},
			},
			inOffsets: []startStableEndOffset{
				{
					start:  0,
					stable: 1,
					end:    1,
				},
				{
					startErr: kerr.ErrorForCode(9),
					stable:   1,
					end:      2,
				},
			},

			expHeaders: []string{
				"partition",
				"leader",
				"epoch",
				"replicas",
				"offline-replicas",
				"load-error",
				"log-start-offset",
				"last-stable-offset",
				"high-watermark",
			},
			expRows: [][]interface{}{
				{int32(0), int32(1), int32(-1), []int32{0}, []int32{}, "-", int64(0), int64(1), int64(1)},
				{int32(1), int32(0), int32(0), []int32{0, 1}, []int32{2, 3}, kerr.ErrorForCode(1), kerr.TypedErrorForCode(9).Message, int64(1), int64(2)},
			},
		},

		{
			name: "no rows",

			inMeta:    []kmsg.MetadataResponseTopicPartition{},
			inOffsets: []startStableEndOffset{},
			expHeaders: []string{
				"partition",
				"leader",
				"epoch",
				"replicas",
				"log-start-offset",
				"high-watermark",
			},
		},

		//
	} {
		t.Run(test.name, func(t *testing.T) {
			headers := describePartitionsHeaders(
				test.inMeta,
				test.inOffsets,
			)
			rows := describePartitionsRows(
				test.inMeta,
				test.inOffsets,
			)

			require.Equal(t, test.expHeaders, headers, "headers")
			require.Equal(t, test.expRows, rows, "rows")
		})
	}
}
