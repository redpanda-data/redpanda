package config

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestFromStructPbToInterface(t *testing.T) {
	tests := []struct {
		name     string
		input    *structpb.Value
		expected any
	}{
		{
			name:     "String value",
			input:    structpb.NewStringValue("test"),
			expected: "test",
		},
		{
			name: "List value",
			input: structpb.NewListValue(&structpb.ListValue{
				Values: []*structpb.Value{
					structpb.NewStringValue("value1"),
					structpb.NewStringValue("value2"),
				},
			}),
			expected: []any{"value1", "value2"},
		},
		{
			name:     "Null value",
			input:    structpb.NewNullValue(),
			expected: nil,
		},
		{
			name:     "Number value",
			input:    structpb.NewNumberValue(123.45),
			expected: 123.45,
		},
		{
			name:     "Boolean value (true)",
			input:    structpb.NewBoolValue(true),
			expected: true,
		},
		{
			name:     "Boolean value (false)",
			input:    structpb.NewBoolValue(false),
			expected: false,
		},
		{
			name: "Struct value",
			input: structpb.NewStructValue(&structpb.Struct{
				Fields: map[string]*structpb.Value{
					"key1": structpb.NewStringValue("value1"),
					"key2": structpb.NewNumberValue(42),
				},
			}),
			expected: map[string]any{
				"key1": "value1",
				"key2": int64(42),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := fromStructPbToInterface(tt.input)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestFilterConfigMap(t *testing.T) {
	configMap := map[string]any{
		"kafka.log.retention.hours":           168,
		"kafka.batch.size":                    16384,
		"redpanda.auto_create_topics_enabled": true,
		"redpanda.developer_mode":             false,
		"log_level":                           "info",
		"cluster_id":                          "test-cluster",
	}

	tests := []struct {
		name           string
		filterPattern  string
		expectedKeys   []string
		expectErr      bool
		expectedErrMsg string
	}{
		{
			name:          "filter kafka keys",
			filterPattern: "kafka.*",
			expectedKeys:  []string{"kafka.log.retention.hours", "kafka.batch.size"},
			expectErr:     false,
		},
		{
			name:          "filter redpanda keys",
			filterPattern: "redpanda.*",
			expectedKeys:  []string{"redpanda.auto_create_topics_enabled", "redpanda.developer_mode"},
			expectErr:     false,
		},
		{
			name:          "filter keys containing 'log'",
			filterPattern: ".*log.*",
			expectedKeys:  []string{"kafka.log.retention.hours", "log_level"},
			expectErr:     false,
		},
		{
			name:          "filter keys ending with 'id'",
			filterPattern: ".*id$",
			expectedKeys:  []string{"cluster_id"},
			expectErr:     false,
		},
		{
			name:          "no matches",
			filterPattern: "nonexistent.*",
			expectedKeys:  []string{},
			expectErr:     false,
		},
		{
			name:          "match all",
			filterPattern: ".*",
			expectedKeys:  []string{"kafka.log.retention.hours", "kafka.batch.size", "redpanda.auto_create_topics_enabled", "redpanda.developer_mode", "log_level", "cluster_id"},
			expectErr:     false,
		},
		{
			name:           "invalid regex",
			filterPattern:  "[",
			expectedKeys:   nil,
			expectErr:      true,
			expectedErrMsg: "invalid regex pattern",
		},
		{
			name:          "case sensitive matching",
			filterPattern: "KAFKA.*",
			expectedKeys:  []string{},
			expectErr:     false,
		},
		{
			name:          "case insensitive matching",
			filterPattern: "(?i)KAFKA.*",
			expectedKeys:  []string{"kafka.log.retention.hours", "kafka.batch.size"},
			expectErr:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := filterConfigMap(configMap, tt.filterPattern)

			if tt.expectErr {
				require.Error(t, err)
				if tt.expectedErrMsg != "" {
					require.Contains(t, err.Error(), tt.expectedErrMsg)
				}
				return
			}

			require.NoError(t, err)

			// Extract keys from result
			var resultKeys []string
			for key := range result {
				resultKeys = append(resultKeys, key)
			}

			require.ElementsMatch(t, tt.expectedKeys, resultKeys)
		})
	}
}

func TestFilterConfigMapEdgeCases(t *testing.T) {
	tests := []struct {
		name          string
		configMap     map[string]any
		filterPattern string
		expectedCount int
	}{
		{
			name:          "empty config map",
			configMap:     map[string]any{},
			filterPattern: ".*",
			expectedCount: 0,
		},
		{
			name: "empty filter pattern",
			configMap: map[string]any{
				"test.key": "value",
			},
			filterPattern: "",
			expectedCount: 1, // empty pattern should match anything
		},
		{
			name: "special regex characters in keys",
			configMap: map[string]any{
				"test.key[0]": "value1",
				"test.key(1)": "value2",
				"test.key*":   "value3",
			},
			filterPattern: `test\.key\[0\]`,
			expectedCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := filterConfigMap(tt.configMap, tt.filterPattern)
			require.NoError(t, err)
			require.Len(t, result, tt.expectedCount)
		})
	}
}

func TestFormatValueForDisplay(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		expected any
	}{
		{
			name:     "whole number float64",
			input:    float64(123),
			expected: int64(123),
		},
		{
			name:     "decimal float64",
			input:    float64(123.45),
			expected: float64(123.45),
		},
		{
			name:     "large whole number float64",
			input:    float64(1000000),
			expected: int64(1000000),
		},
		{
			name:     "string value",
			input:    "test",
			expected: "test",
		},
		{
			name:     "boolean value",
			input:    true,
			expected: true,
		},
		{
			name:     "int value",
			input:    42,
			expected: 42,
		},
		{
			name:     "nil value",
			input:    nil,
			expected: nil,
		},
		{
			name:     "slice value",
			input:    []string{"a", "b", "c"},
			expected: []string{"a", "b", "c"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatValueForDisplay(tt.input)
			require.Equal(t, tt.expected, result)
		})
	}
}
