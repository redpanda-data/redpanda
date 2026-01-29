// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package config

import (
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestProfileToDocumentedYAML(t *testing.T) {
	tests := []struct {
		name     string
		profile  RpkProfile
		contains []string
	}{
		{
			name: "empty profile shows documented fields as comments",
			profile: RpkProfile{
				Name: "test-profile",
			},
			contains: []string{
				"# Available fields are shown below",
				"name: test-profile",
				"# kafka_api:",
				"# admin_api:",
				"# schema_registry:",
				"# description:",
				"# prompt:",
				// Documentation strings from profileFieldDocs
				"# Unique identifier for this profile",
				"# Human-readable description of this profile",
				"# Kafka API connection configuration",
				"# Comma-separated list of broker addresses",
				"# TLS configuration for Kafka API",
				"# SASL authentication configuration",
				"# Admin API connection configuration",
				"# Schema Registry connection configuration",
			},
		},
		{
			name: "profile with kafka brokers shows them and comments for tls/sasl",
			profile: RpkProfile{
				Name:        "kafka-profile",
				Description: "Profile with Kafka config",
				KafkaAPI: RpkKafkaAPI{
					Brokers: []string{"localhost:9092", "localhost:9093"},
				},
			},
			contains: []string{
				"name: kafka-profile",
				"description: Profile with Kafka config",
				"kafka_api:",
				"brokers:",
				`"localhost:9092"`,
				`"localhost:9093"`,
				"# tls:",
				"# sasl:",
			},
		},
		{
			name: "profile with TLS shows TLS fields",
			profile: RpkProfile{
				Name: "tls-profile",
				KafkaAPI: RpkKafkaAPI{
					Brokers: []string{"localhost:9092"},
					TLS: &TLS{
						TruststoreFile: "/path/to/ca.pem",
						CertFile:       "/path/to/cert.pem",
						KeyFile:        "/path/to/key.pem",
					},
				},
			},
			contains: []string{
				"name: tls-profile",
				"tls:",
				"ca_file: /path/to/ca.pem",
				"cert_file: /path/to/cert.pem",
				"key_file: /path/to/key.pem",
			},
		},
		{
			name: "profile with empty TLS struct preserves it",
			profile: RpkProfile{
				Name: "tls-empty",
				KafkaAPI: RpkKafkaAPI{
					Brokers: []string{"localhost:9092"},
					TLS:     &TLS{}, // Empty but non-nil signals TLS enabled
				},
			},
			contains: []string{
				"name: tls-empty",
				"tls: {}",
			},
		},
		{
			name: "profile with SASL shows credentials",
			profile: RpkProfile{
				Name: "sasl-profile",
				KafkaAPI: RpkKafkaAPI{
					Brokers: []string{"localhost:9092"},
					SASL: &SASL{
						User:      "admin",
						Password:  "secret",
						Mechanism: "SCRAM-SHA-256",
					},
				},
			},
			contains: []string{
				"sasl:",
				"user: admin",
				"password: secret",
				"mechanism: SCRAM-SHA-256",
			},
		},
		{
			name: "profile with admin API",
			profile: RpkProfile{
				Name: "admin-profile",
				AdminAPI: RpkAdminAPI{
					Addresses: []string{"localhost:9644"},
				},
			},
			contains: []string{
				"admin_api:",
				"addresses:",
				`"localhost:9644"`,
			},
		},
		{
			name: "profile with schema registry",
			profile: RpkProfile{
				Name: "sr-profile",
				SR: RpkSchemaRegistryAPI{
					Addresses: []string{"localhost:8081"},
				},
			},
			contains: []string{
				"schema_registry:",
				"addresses:",
				`"localhost:8081"`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ProfileToDocumentedYAML(tt.profile)
			require.NoError(t, err)

			resultStr := string(result)
			for _, expected := range tt.contains {
				require.Contains(t, resultStr, expected, "expected output to contain %q", expected)
			}
		})
	}
}

func TestProfileToDocumentedYAML_RoundTrip(t *testing.T) {
	tests := []struct {
		name    string
		profile RpkProfile
	}{
		{
			name: "minimal profile",
			profile: RpkProfile{
				Name: "test-profile",
			},
		},
		{
			name: "full profile",
			profile: RpkProfile{
				Name:        "full-profile",
				Description: "A fully configured profile",
				Prompt:      "custom-prompt",
				KafkaAPI: RpkKafkaAPI{
					Brokers: []string{"localhost:9092", "broker2:9092"},
					TLS: &TLS{
						TruststoreFile:     "/path/to/ca.pem",
						CertFile:           "/path/to/cert.pem",
						KeyFile:            "/path/to/key.pem",
						InsecureSkipVerify: false,
					},
					SASL: &SASL{
						User:      "user",
						Password:  "pass",
						Mechanism: "SCRAM-SHA-512",
					},
				},
				AdminAPI: RpkAdminAPI{
					Addresses: []string{"localhost:9644"},
					TLS: &TLS{
						TruststoreFile: "/path/to/admin-ca.pem",
					},
				},
				SR: RpkSchemaRegistryAPI{
					Addresses: []string{"localhost:8081"},
				},
			},
		},
		{
			name: "special characters in values",
			profile: RpkProfile{
				Name:        "special-chars",
				Description: "Description with: colon and 'quotes'",
				KafkaAPI: RpkKafkaAPI{
					Brokers: []string{"host:9092"},
					SASL: &SASL{
						User:     "user@domain",
						Password: "pass:word!@#",
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			yamlBytes, err := ProfileToDocumentedYAML(tt.profile)
			require.NoError(t, err)

			var parsed RpkProfile
			err = yaml.Unmarshal(yamlBytes, &parsed)
			require.NoError(t, err)

			require.Equal(t, tt.profile, parsed)
		})
	}
}

func TestProfileToDocumentedYAML_ExcludesInternalFields(t *testing.T) {
	profile := RpkProfile{
		Name:      "internal-test",
		FromCloud: true,
		CloudCluster: RpkCloudCluster{
			ClusterID:   "test-cluster",
			ClusterName: "test",
		},
		CloudEnvironment: "production",
	}

	result, err := ProfileToDocumentedYAML(profile)
	require.NoError(t, err)

	resultStr := string(result)

	// Cloud fields appear so they're preserved on edit
	require.Contains(t, resultStr, "from_cloud: true")
	require.Contains(t, resultStr, "cloud_cluster:")
	require.Contains(t, resultStr, "cluster_id: test-cluster")
	require.Contains(t, resultStr, "cluster_name: test")

	// Excluded fields should not appear
	for field := range excludedFields {
		require.NotContains(t, resultStr, field, "internal field %q should be excluded", field)
	}

	// Empty Cloud fields should have NO documentation comments
	require.NotContains(t, resultStr, "# auth_org_id")
	require.NotContains(t, resultStr, "# cluster_type")
}

func TestNeedsQuoting(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		{"empty string", "", true},
		{"simple string", "simple", false},
		{"string with space", "with space", false},
		{"string with colon", "with:colon", true},
		{"yaml bool true", "true", true},
		{"yaml bool false", "false", true},
		{"yaml null", "null", true},
		{"yaml bool yes", "yes", true},
		{"yaml bool no", "no", true},
		{"host:port", "localhost:9092", true},
		{"file path", "/path/to/file.pem", false},
		{"mechanism", "SCRAM-SHA-256", false},
		{"leading space", " leading-space", true},
		{"trailing space", "trailing-space ", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := needsQuoting(tt.input)
			require.Equal(t, tt.expected, result, "needsQuoting(%q)", tt.input)
		})
	}
}

func TestIsEmptyValue(t *testing.T) {
	tests := []struct {
		name     string
		value    interface{}
		expected bool
	}{
		{"empty string", "", true},
		{"non-empty string", "hello", false},
		{"zero int", 0, true},
		{"non-zero int", 42, false},
		{"false bool", false, true},
		{"true bool", true, false},
		{"nil slice", []string(nil), true},
		{"empty slice", []string{}, true},
		{"non-empty slice", []string{"a"}, false},
		{"nil map", map[string]string(nil), true},
		{"empty map", map[string]string{}, true},
		{"non-empty map", map[string]string{"a": "b"}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isEmptyValue(reflect.ValueOf(tt.value))
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestProfileFieldDocsCompleteness(t *testing.T) {
	// This test ensures all fields in RpkProfile are documented in
	// profileFieldDocs, excludedFields, or noCommentFields. When adding new
	// fields to RpkProfile, you must add documentation for them.
	var missing []string
	collectUndocumentedFields(reflect.TypeOf(RpkProfile{}), "", &missing)

	if len(missing) > 0 {
		t.Errorf("The following fields are missing documentation in profileFieldDocs, excludedFields, or noCommentFields:\n  %s",
			strings.Join(missing, "\n  "))
	}
}

// collectUndocumentedFields recursively collects all YAML field paths that are
// not documented.
func collectUndocumentedFields(t reflect.Type, pathPrefix string, missing *[]string) {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return
	}

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)

		// Skip unexported fields
		if !field.IsExported() {
			continue
		}

		// Get yaml tag
		yamlTag := field.Tag.Get("yaml")
		if yamlTag == "" || yamlTag == "-" {
			continue
		}

		// Parse yaml tag to get field name
		tagParts := strings.Split(yamlTag, ",")
		fieldName := tagParts[0]
		if fieldName == "" {
			fieldName = strings.ToLower(field.Name)
		}

		// Build full path
		fullPath := fieldName
		if pathPrefix != "" {
			fullPath = pathPrefix + "." + fieldName
		}

		// Check if documented, excluded, or in noCommentFields
		_, inDocs := profileFieldDocs[fullPath]
		_, isExcluded := excludedFields[fullPath]
		isNoComment := isNoCommentField(fullPath)

		if !inDocs && !isExcluded && !isNoComment {
			*missing = append(*missing, fullPath)
		}

		// Skip recursing into excluded or noComment fields - their nested fields are also excluded/no-comment
		if isExcluded || isNoComment {
			continue
		}

		// Recurse into nested structs
		fieldType := field.Type
		if fieldType.Kind() == reflect.Ptr {
			fieldType = fieldType.Elem()
		}
		if fieldType.Kind() == reflect.Struct {
			// Skip time.Duration wrapper and other non-config structs
			if fieldType.PkgPath() != "" && !strings.Contains(fieldType.PkgPath(), "config") {
				continue
			}
			collectUndocumentedFields(fieldType, fullPath, missing)
		}
	}
}
