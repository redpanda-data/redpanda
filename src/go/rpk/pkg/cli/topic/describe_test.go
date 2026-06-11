package topic

import (
	"bytes"
	"encoding/json"
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
			expected: []describeTopicPartition{
				{
					Partition:        0,
					Leader:           1,
					Epoch:            5,
					Replicas:         []int32{1, 2, 3},
					OfflineReplicas:  []int32{},
					LogStartOffset:   0,
					LastStableOffset: 100,
					HighWatermark:    100,
				},
				{
					Partition:        1,
					Leader:           2,
					Epoch:            3,
					Replicas:         []int32{1, 2, 3},
					OfflineReplicas:  []int32{3},
					LogStartOffset:   50,
					LastStableOffset: 150,
					HighWatermark:    200,
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
			expected: []describeTopicPartition{
				{
					Partition:       0,
					Leader:          1,
					Epoch:           5,
					Replicas:        []int32{1, 2, 3},
					OfflineReplicas: []int32{},
					LoadError:       "REPLICA_NOT_AVAILABLE: The replica is not available for the requested topic-partition.",
					LogStartOffset:  -1,
					HighWatermark:   -1,
					Errors:          []string{"REPLICA_NOT_AVAILABLE", "REPLICA_NOT_AVAILABLE"},
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
					startErr: kerr.ErrorForCode(3),
					stable:   -1,
					end:      -1,
					endErr:   kerr.ErrorForCode(3),
				},
			},
			expected: []describeTopicPartition{
				{
					Partition:        0,
					Leader:           -1,
					LoadError:        "UNKNOWN_TOPIC_OR_PARTITION: This server does not host this topic-partition.",
					LogStartOffset:   -1,
					LastStableOffset: -1, // stable=-1 with no error → populated as-is
					Replicas:         []int32{},
					OfflineReplicas:  []int32{},
					HighWatermark:    -1,
					Errors:           []string{"UNKNOWN_TOPIC_OR_PARTITION", "UNKNOWN_TOPIC_OR_PARTITION"},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := buildDescribeTopicPartitions(tc.partitions, tc.offsets)
			assert.Equal(t, tc.expected, result)
		})
	}
}

// TestPrintDescribedTopicsNonText verifies that JSON and YAML output shapes are
// preserved after the migration to out.Render. The json: and yaml: tags on
// describedTopic and its children are unchanged, so the serialised form must
// match the pre-migration output exactly.
func TestPrintDescribedTopicsNonText(t *testing.T) {
	testCases := []struct {
		name           string
		format         string
		topics         []describedTopic
		expectedOutput string
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
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			f := config.OutFormatter{Kind: tc.format}

			err := printDescribedTopics(f, tc.topics, &buf)
			require.NoError(t, err)

			switch tc.format {
			case "json":
				var expected, actual any
				require.NoError(t, json.Unmarshal([]byte(tc.expectedOutput), &expected))
				require.NoError(t, json.Unmarshal(buf.Bytes(), &actual))
				assert.Equal(t, expected, actual)
			case "yaml":
				assert.Equal(t, strings.TrimRight(tc.expectedOutput, "\n"), strings.TrimRight(buf.String(), "\n"))
			}
		})
	}
}

// TestPrintDescribedTopics verifies text-mode output of the new printDescribedTopics.
// All sections (SUMMARY, CONFIGS, PARTITIONS) are now rendered declaratively via
// header: tags. The PARTITIONS section renders as a table with table: tags; wide
// columns (OFFLINE-REPLICAS, LOAD-ERROR, LAST-STABLE-OFFSET) are hidden in default mode.
func TestPrintDescribedTopics(t *testing.T) {
	testCases := []struct {
		name           string
		topics         []describedTopic
		expectedOutput string
	}{
		{
			// PARTITIONS section renders via header: tag with table: tagged rows.
			name: "Print all sections",
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
			expectedOutput: "SUMMARY\n=======\nNAME        test-topic\nPARTITIONS  2\nREPLICAS    3\n\nCONFIGS\n=======\nKEY           VALUE      SOURCE\nretention.ms  604800000  DEFAULT_CONFIG\n\nPARTITIONS\n==========\nPARTITION  LEADER  EPOCH  REPLICAS  LOG-START-OFFSET  HIGH-WATERMARK\n0          1       0      [1 2 3]   0                 0\n1          2       0      [2 3 1]   0                 0\n",
		},
		{
			// INTERNAL is omitempty: false → omitted. Configs and Partitions are nil → sections omitted.
			name: "Print only summary",
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
			expectedOutput: "SUMMARY\n=======\nNAME        test-topic\nINTERNAL    true\nPARTITIONS  1\nREPLICAS    1\n",
		},
		{
			name: "Print summary and configs",
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
			expectedOutput: "SUMMARY\n=======\nNAME        test-topic\nPARTITIONS  1\nREPLICAS    1\n\nCONFIGS\n=======\nKEY               VALUE     SOURCE\ncleanup.policy    delete    DEFAULT_CONFIG\ncompression.type  producer  DYNAMIC_TOPIC_CONFIG\n",
		},
		{
			// Configs and Partitions are nil (omitempty → omitted). REPLICAS=0 is omitempty → omitted.
			name: "Print with errors",
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
			expectedOutput: "SUMMARY\n=======\nNAME        error-topic\nPARTITIONS  0\nERROR       UNKNOWN_TOPIC_OR_PARTITION\n",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			f := config.OutFormatter{Kind: "text"}

			err := printDescribedTopics(f, tc.topics, &buf)
			require.NoError(t, err)

			assert.Equal(t, tc.expectedOutput, buf.String())
		})
	}
}

func TestDescribedTopicJSONShapeStable(t *testing.T) {
	d := describedTopic{
		Summary: describeTopicSummary{
			Name:       "t",
			Internal:   false,
			Partitions: 1,
			Replicas:   3,
		},
		Configs: []describeTopicConfig{
			{Key: "cleanup.policy", Value: "delete", Source: "DYNAMIC"},
		},
		Partitions: []describeTopicPartition{
			{
				Partition:      0,
				Leader:         1,
				Epoch:          0,
				Replicas:       []int32{1, 2, 3},
				LogStartOffset: 0,
				HighWatermark:  100,
			},
		},
	}
	b, err := json.Marshal(d)
	require.NoError(t, err)
	require.JSONEq(t, `{
		"summary":{"name":"t","internal":false,"partitions":1,"replicas":3,"error":""},
		"configs":[{"key":"cleanup.policy","value":"delete","source":"DYNAMIC"}],
		"partitions":[{"partition":0,"leader":1,"epoch":0,"replicas":[1,2,3],"log_start_offset":0,"high_watermark":100}]
	}`, string(b))
}

// TestDescribedTopicJSONShapeWithOmitEmptyPopulated verifies that every
// omitempty field on describeTopicPartition and the non-empty summary Error
// appear in the serialised output with the correct json: key names.
// Renaming or dropping any json tag will break this test.
func TestDescribedTopicJSONShapeWithOmitEmptyPopulated(t *testing.T) {
	d := describedTopic{
		Summary: describeTopicSummary{
			Name:       "t",
			Internal:   true,
			Partitions: 1,
			Replicas:   3,
			Error:      "UNKNOWN_TOPIC",
		},
		Configs: []describeTopicConfig{
			{Key: "cleanup.policy", Value: "delete", Source: "DYNAMIC"},
		},
		Partitions: []describeTopicPartition{
			{
				Partition:        0,
				Leader:           1,
				Epoch:            2,
				Replicas:         []int32{1, 2, 3},
				OfflineReplicas:  []int32{3},
				LoadError:        "REPLICA_NOT_AVAILABLE",
				LogStartOffset:   10,
				LastStableOffset: 42,
				HighWatermark:    100,
				Errors:           []string{"something"},
			},
		},
	}
	b, err := json.Marshal(d)
	require.NoError(t, err)
	require.JSONEq(t, `{
		"summary":{"name":"t","internal":true,"partitions":1,"replicas":3,"error":"UNKNOWN_TOPIC"},
		"configs":[{"key":"cleanup.policy","value":"delete","source":"DYNAMIC"}],
		"partitions":[{
			"partition":0,"leader":1,"epoch":2,"replicas":[1,2,3],
			"offline_replicas":[3],"load_error":"REPLICA_NOT_AVAILABLE",
			"log_start_offset":10,"last_stable_offset":42,"high_watermark":100,
			"error":["something"]
		}]
	}`, string(b))
}
