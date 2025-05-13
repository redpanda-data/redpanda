package config

import (
	"testing"

	"github.com/stretchr/testify/require"

	"google.golang.org/protobuf/types/known/structpb"
)

func TestFromStructPbToString(t *testing.T) {
	tests := []struct {
		name     string
		input    *structpb.Value
		expected string
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
			expected: "value1, value2",
		},
		{
			name:     "Null value",
			input:    structpb.NewNullValue(),
			expected: "null",
		},
		{
			name:     "Number value",
			input:    structpb.NewNumberValue(123.45),
			expected: "123.45",
		},
		{
			name:     "String value",
			input:    structpb.NewStringValue("example"),
			expected: "example",
		},
		{
			name:     "Boolean value (true)",
			input:    structpb.NewBoolValue(true),
			expected: "true",
		},
		{
			name:     "Boolean value (false)",
			input:    structpb.NewBoolValue(false),
			expected: "false",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := fromStructPbToString(tt.input)
			if result != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, result)
			}
		})
	}
}

func TestFromStructPbToStringWithStruct(t *testing.T) {
	tests := []struct {
		name     string
		input    *structpb.Value
		expected string
	}{
		{
			name: "Struct value",
			input: structpb.NewStructValue(&structpb.Struct{
				Fields: map[string]*structpb.Value{
					"key1": structpb.NewStringValue("value1"),
					"key2": structpb.NewNumberValue(42),
				},
			}),
			expected: `{"key1":"value1", "key2":42}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := fromStructPbToString(tt.input)
			require.JSONEq(t, tt.expected, result, "expected JSON output does not match")
		})
	}
}
