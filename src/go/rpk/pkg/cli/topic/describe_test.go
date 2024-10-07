package topic

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestBuildDescribeTopicPartitions(t *testing.T) {
	testCases := []struct {
		name       string
		partitions []kmsg.MetadataResponseTopicPartition
		offsets    []startStableEndOffset
		uses       uses
		expUseErr  bool
		expected   []describeTopicPartition
	}{
		{
			name: "Normal case",
			partitions: []kmsg.MetadataResponseTopicPartition{
				{
					Partition:   0,
					Leader:      1,
					LeaderEpoch: 5,
					Replicas:    []int32{1, 2, 3},
				},
				{
					Partition:       1,
					Leader:          2,
					LeaderEpoch:     3,
					Replicas:        []int32{1, 2, 3},
					OfflineReplicas: []int32{3},
				},
			},
			offsets: []startStableEndOffset{
				{start: 0, stable: 100, end: 100, startErr: nil, stableErr: nil, endErr: nil},
				{start: 50, stable: 150, end: 200, startErr: nil, stableErr: nil, endErr: nil},
			},
			uses: uses{Offline: true, Stable: true},
			expected: []describeTopicPartition{
				{
					Partition:            0,
					Leader:               1,
					Epoch:                5,
					Replicas:             []int32{1, 2, 3},
					OfflineReplicas:      []int32{},
					LogStartOffset:       0,
					logStartOffsetText:   int64(0),
					LastStableOffset:     100,
					lastStableOffsetText: int64(100),
					HighWatermark:        100,
					highWatermarkText:    int64(100),
				},
				{
					Partition:            1,
					Leader:               2,
					Epoch:                3,
					Replicas:             []int32{1, 2, 3},
					OfflineReplicas:      []int32{3},
					LogStartOffset:       50,
					logStartOffsetText:   int64(50),
					LastStableOffset:     150,
					lastStableOffsetText: int64(150),
					HighWatermark:        200,
					highWatermarkText:    int64(200),
				},
			},
		},
		{
			name: "With errors",
			partitions: []kmsg.MetadataResponseTopicPartition{
				{
					Partition:   0,
					Leader:      1,
					LeaderEpoch: 5,
					Replicas:    []int32{1, 2, 3},
					ErrorCode:   9, // REPLICA_NOT_AVAILABLE error code
				},
			},
			offsets: []startStableEndOffset{
				{
					start: -1, stable: -1, end: -1,
					startErr: kerr.ErrorForCode(9), stableErr: errUnlisted, endErr: kerr.ErrorForCode(9),
				},
			},
			uses:      uses{LoadErr: true},
			expUseErr: true,
			expected: []describeTopicPartition{
				{
					Partition:          0,
					Leader:             1,
					Epoch:              5,
					Replicas:           []int32{1, 2, 3},
					LoadError:          "REPLICA_NOT_AVAILABLE: The replica is not available for the requested topic-partition.",
					LogStartOffset:     -1,
					logStartOffsetText: "REPLICA_NOT_AVAILABLE",
					HighWatermark:      -1,
					highWatermarkText:  "REPLICA_NOT_AVAILABLE",
					Errors:             []string{"REPLICA_NOT_AVAILABLE", "REPLICA_NOT_AVAILABLE"},
				},
			},
		},
		{
			name: "Recovery failure - Unknown topic or partition",
			partitions: []kmsg.MetadataResponseTopicPartition{
				{
					Partition: 0,
					Leader:    -1, // No leader due to failed recovery
					ErrorCode: 3,  // UNKNOWN_TOPIC_OR_PARTITION error code
				},
			},
			offsets: []startStableEndOffset{
				{
					start:    -1,
					startErr: kerr.ErrorForCode(3), // Set the error
					stable:   -1,
					end:      -1,
					endErr:   kerr.ErrorForCode(3),
				},
			},
			uses:      uses{LoadErr: true},
			expUseErr: true,
			expected: []describeTopicPartition{
				{
					Partition:          0,
					Leader:             -1,
					LoadError:          "UNKNOWN_TOPIC_OR_PARTITION: This server does not host this topic-partition.",
					LogStartOffset:     -1,
					logStartOffsetText: "UNKNOWN_TOPIC_OR_PARTITION",
					Replicas:           []int32{},
					HighWatermark:      -1,
					highWatermarkText:  "UNKNOWN_TOPIC_OR_PARTITION",
					Errors:             []string{"UNKNOWN_TOPIC_OR_PARTITION", "UNKNOWN_TOPIC_OR_PARTITION"},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := buildDescribeTopicPartitions(tc.partitions, tc.offsets, tc.uses)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestPartitionHeaderAndRow(t *testing.T) {
	tests := []struct {
		name           string
		partition      describeTopicPartition
		uses           uses
		expectedHeader []string
		expectedRow    []string
	}{
		{
			name: "all fields",
			partition: describeTopicPartition{
				Partition:            0,
				Leader:               1,
				Epoch:                5,
				Replicas:             []int32{1, 2, 3},
				OfflineReplicas:      []int32{3},
				LoadError:            "REPLICA_NOT_AVAILABLE",
				LogStartOffset:       100,
				logStartOffsetText:   int64(100),
				LastStableOffset:     200,
				lastStableOffsetText: int64(200),
				HighWatermark:        300,
				highWatermarkText:    int64(300),
				Errors:               []string{"Error1", "Error2"},
			},
			uses: uses{Offline: true, LoadErr: true, Stable: true},
			expectedHeader: []string{
				"partition", "leader", "epoch", "replicas", "offline-replicas",
				"load-error", "log-start-offset", "last-stable-offset", "high-watermark",
			},
			expectedRow: []string{
				"0", "1", "5", "[1 2 3]", "[3]", "REPLICA_NOT_AVAILABLE",
				"100", "200", "300",
			},
		},
		{
			name: "minimal fields",
			partition: describeTopicPartition{
				Partition:          1,
				Leader:             2,
				Epoch:              3,
				Replicas:           []int32{1, 2},
				LogStartOffset:     50,
				logStartOffsetText: int64(50),
				HighWatermark:      150,
				highWatermarkText:  int64(150),
			},
			uses: uses{},
			expectedHeader: []string{
				"partition", "leader", "epoch", "replicas", "log-start-offset", "high-watermark",
			},
			expectedRow: []string{
				"1", "2", "3", "[1 2]", "50", "150",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			header := partitionHeader(tt.uses)
			assert.Equal(t, tt.expectedHeader, header, "Headers do not match expected")

			row := tt.partition.Row(tt.uses)
			assert.Equal(t, tt.expectedRow, row, "Row does not match expected")
		})
	}
}

func TestGetDescribeUsed(t *testing.T) {
	testCases := []struct {
		name       string
		partitions []kmsg.MetadataResponseTopicPartition
		offsets    []startStableEndOffset
		expected   uses
	}{
		{
			name: "No special cases",
			partitions: []kmsg.MetadataResponseTopicPartition{
				{Partition: 0, ErrorCode: 0},
			},
			offsets: []startStableEndOffset{
				{start: 0, stable: 100, end: 100},
			},
			expected: uses{},
		},
		{
			name: "With offline replicas",
			partitions: []kmsg.MetadataResponseTopicPartition{
				{Partition: 0, OfflineReplicas: []int32{1}},
			},
			offsets: []startStableEndOffset{
				{start: 0, stable: 100, end: 100},
			},
			expected: uses{Offline: true},
		},
		{
			name: "With load error",
			partitions: []kmsg.MetadataResponseTopicPartition{
				{Partition: 0, ErrorCode: 1},
			},
			offsets: []startStableEndOffset{
				{start: 0, stable: 100, end: 100},
			},
			expected: uses{LoadErr: true},
		},
		{
			name: "With stable offset different from end",
			partitions: []kmsg.MetadataResponseTopicPartition{
				{Partition: 0},
			},
			offsets: []startStableEndOffset{
				{start: 0, stable: 50, end: 100, stableErr: nil, endErr: nil},
			},
			expected: uses{Stable: true},
		},
		{
			name: "All cases",
			partitions: []kmsg.MetadataResponseTopicPartition{
				{Partition: 0, ErrorCode: 1, OfflineReplicas: []int32{1}},
			},
			offsets: []startStableEndOffset{
				{start: 0, stable: 50, end: 100, stableErr: nil, endErr: nil},
			},
			expected: uses{Offline: true, LoadErr: true, Stable: true},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := getDescribeUsed(tc.partitions, tc.offsets)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestPrintDescribedTopicsFormatter(t *testing.T) {
	testCases := []struct {
		name           string
		format         string
		topics         []describedTopic
		expectedOutput string
		expectedReturn bool
	}{
		{
			name:   "JSON format - single topic",
			format: "json",
			topics: []describedTopic{
				{
					Summary: describeTopicSummary{
						Name:       "test-topic",
						Internal:   false,
						Partitions: 3,
						Replicas:   2,
					},
					Configs: []describeTopicConfig{
						{Key: "retention.ms", Value: "604800000", Source: "DEFAULT_CONFIG"},
					},
					Partitions: []describeTopicPartition{
						{Partition: 0, Leader: 1, Replicas: []int32{1, 2}},
						{Partition: 1, Leader: 2, Replicas: []int32{2, 1}},
						{Partition: 2, Leader: 1, Replicas: []int32{1, 2}},
					},
				},
			},
			expectedOutput: `[{"summary":{"name":"test-topic","internal":false,"partitions":3,"replicas":2,"error":""},"configs":[{"key":"retention.ms","value":"604800000","source":"DEFAULT_CONFIG"}],"partitions":[{"partition":0,"leader":1,"epoch":0,"replicas":[1,2],"log_start_offset":0,"high_watermark":0},{"partition":1,"leader":2,"epoch":0,"replicas":[2,1],"log_start_offset":0,"high_watermark":0},{"partition":2,"leader":1,"epoch":0,"replicas":[1,2],"log_start_offset":0,"high_watermark":0}]}]`,
			expectedReturn: true,
		},
		{
			name:   "JSON format - multiple topics",
			format: "json",
			topics: []describedTopic{
				{
					Summary: describeTopicSummary{
						Name:       "topic1",
						Internal:   false,
						Partitions: 2,
						Replicas:   2,
					},
					Configs: []describeTopicConfig{
						{Key: "retention.ms", Value: "86400000", Source: "DYNAMIC_TOPIC_CONFIG"},
					},
					Partitions: []describeTopicPartition{
						{Partition: 0, Leader: 1, Replicas: []int32{1, 2}},
						{Partition: 1, Leader: 2, Replicas: []int32{2, 1}},
					},
				},
				{
					Summary: describeTopicSummary{
						Name:       "topic2",
						Internal:   true,
						Partitions: 1,
						Replicas:   3,
					},
					Configs: []describeTopicConfig{
						{Key: "cleanup.policy", Value: "compact", Source: "STATIC_BROKER_CONFIG"},
					},
					Partitions: []describeTopicPartition{
						{Partition: 0, Leader: 3, Replicas: []int32{1, 2, 3}},
					},
				},
			},
			expectedOutput: `[{"summary":{"name":"topic1","internal":false,"partitions":2,"replicas":2,"error":""},"configs":[{"key":"retention.ms","value":"86400000","source":"DYNAMIC_TOPIC_CONFIG"}],"partitions":[{"partition":0,"leader":1,"epoch":0,"replicas":[1,2],"log_start_offset":0,"high_watermark":0},{"partition":1,"leader":2,"epoch":0,"replicas":[2,1],"log_start_offset":0,"high_watermark":0}]},{"summary":{"name":"topic2","internal":true,"partitions":1,"replicas":3,"error":""},"configs":[{"key":"cleanup.policy","value":"compact","source":"STATIC_BROKER_CONFIG"}],"partitions":[{"partition":0,"leader":3,"epoch":0,"replicas":[1,2,3],"log_start_offset":0,"high_watermark":0}]}]`,
			expectedReturn: true,
		},
		{
			name:   "JSON format - topics with errors",
			format: "json",
			topics: []describedTopic{
				{
					Summary: describeTopicSummary{
						Name:       "error-topic-1",
						Internal:   false,
						Partitions: 0,
						Replicas:   0,
						Error:      "UNKNOWN_TOPIC_OR_PARTITION",
					},
					Configs:    []describeTopicConfig{},
					Partitions: []describeTopicPartition{},
				},
				{
					Summary: describeTopicSummary{
						Name:       "partial-error-topic",
						Internal:   false,
						Partitions: 2,
						Replicas:   3,
						Error:      "",
					},
					Configs: []describeTopicConfig{
						{Key: "min.insync.replicas", Value: "2", Source: "DYNAMIC_TOPIC_CONFIG"},
					},
					Partitions: []describeTopicPartition{
						{
							Partition:       0,
							Leader:          1,
							Replicas:        []int32{1, 2, 3},
							OfflineReplicas: []int32{3},
							LogStartOffset:  100,
							HighWatermark:   200,
						},
						{
							Partition:      1,
							Leader:         -1,
							Replicas:       []int32{1, 2, 3},
							LogStartOffset: -1,
							HighWatermark:  -1,
							LoadError:      "LEADER_NOT_AVAILABLE",
							Errors:         []string{"LEADER_NOT_AVAILABLE"},
						},
					},
				},
				{
					Summary: describeTopicSummary{
						Name:       "normal-topic",
						Internal:   false,
						Partitions: 1,
						Replicas:   1,
					},
					Configs: []describeTopicConfig{
						{Key: "retention.ms", Value: "86400000", Source: "DEFAULT_CONFIG"},
					},
					Partitions: []describeTopicPartition{
						{
							Partition:      0,
							Leader:         1,
							Replicas:       []int32{1},
							LogStartOffset: 0,
							HighWatermark:  150,
						},
					},
				},
			},
			expectedOutput: `[
				{
					"summary": {
						"name": "error-topic-1",
						"internal": false,
						"partitions": 0,
						"replicas": 0,
						"error": "UNKNOWN_TOPIC_OR_PARTITION"
					},
					"configs": [],
					"partitions": []
				},
				{
					"summary": {
						"name": "partial-error-topic",
						"internal": false,
						"partitions": 2,
						"replicas": 3,
						"error": ""
					},
					"configs": [
						{
							"key": "min.insync.replicas",
							"value": "2",
							"source": "DYNAMIC_TOPIC_CONFIG"
						}
					],
					"partitions": [
						{
							"partition": 0,
							"leader": 1,
							"epoch": 0,
							"replicas": [1, 2, 3],
							"offline_replicas": [3],
							"log_start_offset": 100,
							"high_watermark": 200
						},
						{
							"partition": 1,
							"leader": -1,
							"epoch": 0,
							"replicas": [1, 2, 3],
							"log_start_offset": -1,
							"high_watermark": -1,
							"load_error": "LEADER_NOT_AVAILABLE",
							"error": ["LEADER_NOT_AVAILABLE"]
						}
					]
				},
				{
					"summary": {
						"name": "normal-topic",
						"internal": false,
						"partitions": 1,
						"replicas": 1,
						"error": ""
					},
					"configs": [
						{
							"key": "retention.ms",
							"value": "86400000",
							"source": "DEFAULT_CONFIG"
						}
					],
					"partitions": [
						{
							"partition": 0,
							"leader": 1,
							"epoch": 0,
							"replicas": [1],
							"log_start_offset": 0,
							"high_watermark": 150
						}
					]
				}
			]`,
			expectedReturn: true,
		},
		{
			name:   "YAML format - single topic",
			format: "yaml",
			topics: []describedTopic{
				{
					Summary: describeTopicSummary{
						Name:       "test-topic",
						Internal:   false,
						Partitions: 1,
						Replicas:   1,
					},
					Configs: []describeTopicConfig{
						{Key: "compression.type", Value: "producer", Source: "DEFAULT_CONFIG"},
					},
					Partitions: []describeTopicPartition{
						{Partition: 0, Leader: 1, Replicas: []int32{1}},
					},
				},
			},
			expectedOutput: `- summary:
    name: test-topic
    internal: false
    partitions: 1
    replicas: 1
    error: ""
  configs:
    - key: compression.type
      value: producer
      source: DEFAULT_CONFIG
  partitions:
    - partition: 0
      leader: 1
      epoch: 0
      replicas:
        - 1
      log_start_offset: 0
      high_watermark: 0`,
			expectedReturn: true,
		},
		{
			name:   "YAML format - topic with error",
			format: "yaml",
			topics: []describedTopic{
				{
					Summary: describeTopicSummary{
						Name:       "error-topic",
						Internal:   false,
						Partitions: 0,
						Replicas:   0,
						Error:      "UNKNOWN_TOPIC_OR_PARTITION",
					},
				},
			},
			expectedOutput: `- summary:
    name: error-topic
    internal: false
    partitions: 0
    replicas: 0
    error: UNKNOWN_TOPIC_OR_PARTITION
  configs: []
  partitions: []`,
			expectedReturn: true,
		},
		{
			name:   "Text format - should return false",
			format: "text",
			topics: []describedTopic{
				{
					Summary: describeTopicSummary{
						Name:       "test-topic",
						Internal:   false,
						Partitions: 1,
						Replicas:   1,
					},
				},
			},
			expectedOutput: "",
			expectedReturn: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			f := config.OutFormatter{Kind: tc.format}

			result := printDescribedTopicsFormatter(f, tc.topics, &buf)

			assert.Equal(t, tc.expectedReturn, result)

			if tc.expectedReturn {
				switch tc.format {
				case "json":
					var expected, actual interface{}
					err := json.Unmarshal([]byte(tc.expectedOutput), &expected)
					require.NoError(t, err)
					err = json.Unmarshal(buf.Bytes(), &actual)
					require.NoError(t, err)
					assert.Equal(t, expected, actual)
				case "yaml":
					assert.Equal(t, strings.TrimRight(tc.expectedOutput, "\n"), strings.TrimRight(buf.String(), "\n"))
				default:
					assert.Equal(t, tc.expectedOutput, buf.String())
				}
			} else {
				assert.Empty(t, buf.String())
			}
		})
	}
}

func TestPrintDescribedTopics(t *testing.T) {
	testCases := []struct {
		name           string
		summary        bool
		configs        bool
		partitions     bool
		topics         []describedTopic
		expectedOutput string
	}{
		{
			name:       "Print all sections",
			summary:    true,
			configs:    true,
			partitions: true,
			topics: []describedTopic{
				{
					Summary: describeTopicSummary{
						Name:       "test-topic",
						Internal:   false,
						Partitions: 2,
						Replicas:   3,
					},
					Configs: []describeTopicConfig{
						{Key: "retention.ms", Value: "604800000", Source: "DEFAULT_CONFIG"},
					},
					Partitions: []describeTopicPartition{
						{Partition: 0, Leader: 1, Replicas: []int32{1, 2, 3}},
						{Partition: 1, Leader: 2, Replicas: []int32{2, 3, 1}},
					},
				},
			},
			expectedOutput: `SUMMARY
=======
NAME        test-topic
PARTITIONS  2
REPLICAS    3

CONFIGS
=======
KEY           VALUE      SOURCE
retention.ms  604800000  DEFAULT_CONFIG

PARTITIONS
==========
PARTITION  LEADER  EPOCH  REPLICAS  LOG-START-OFFSET  HIGH-WATERMARK
0          1       0      [1 2 3]   <nil>             <nil>
1          2       0      [2 3 1]   <nil>             <nil>
`,
		},
		{
			name:       "Print only summary",
			summary:    true,
			configs:    false,
			partitions: false,
			topics: []describedTopic{
				{
					Summary: describeTopicSummary{
						Name:       "test-topic",
						Internal:   true,
						Partitions: 1,
						Replicas:   1,
					},
				},
			},
			expectedOutput: `NAME        test-topic
INTERNAL    true
PARTITIONS  1
REPLICAS    1
`,
		},
		{
			name:       "Print summary and configs",
			summary:    true,
			configs:    true,
			partitions: false,
			topics: []describedTopic{
				{
					Summary: describeTopicSummary{
						Name:       "test-topic",
						Internal:   false,
						Partitions: 1,
						Replicas:   1,
					},
					Configs: []describeTopicConfig{
						{Key: "cleanup.policy", Value: "delete", Source: "DEFAULT_CONFIG"},
						{Key: "compression.type", Value: "producer", Source: "DYNAMIC_TOPIC_CONFIG"},
					},
				},
			},
			expectedOutput: `SUMMARY
=======
NAME        test-topic
PARTITIONS  1
REPLICAS    1

CONFIGS
=======
KEY               VALUE     SOURCE
cleanup.policy    delete    DEFAULT_CONFIG
compression.type  producer  DYNAMIC_TOPIC_CONFIG
`,
		},
		{
			name:       "Print with errors",
			summary:    true,
			configs:    true,
			partitions: true,
			topics: []describedTopic{
				{
					Summary: describeTopicSummary{
						Name:       "error-topic",
						Internal:   false,
						Partitions: 0,
						Replicas:   0,
						Error:      "UNKNOWN_TOPIC_OR_PARTITION",
					},
				},
			},
			expectedOutput: `SUMMARY
=======
NAME        error-topic
PARTITIONS  0
ERROR       UNKNOWN_TOPIC_OR_PARTITION

CONFIGS
=======
KEY   VALUE  SOURCE

PARTITIONS
==========
PARTITION  LEADER  EPOCH  REPLICAS  LOG-START-OFFSET  HIGH-WATERMARK
`,
		},
	}

	for _, tc := range testCases {
		// Janky way to test the output of a function that prints to stdout. Would be preferable to just pass in a buf and check that.
		t.Run(tc.name, func(t *testing.T) {
			// Redirect stdout to capture output
			old := os.Stdout
			r, w, _ := os.Pipe()
			os.Stdout = w

			printDescribedTopics(tc.summary, tc.configs, tc.partitions, tc.topics)

			// Restore stdout
			w.Close()
			os.Stdout = old

			var buf bytes.Buffer
			io.Copy(&buf, r)
			output := buf.String()

			// Compare output
			assert.Equal(t, tc.expectedOutput, output)
		})
	}
}
