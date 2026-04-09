// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package shadow

import (
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	adminv2 "buf.build/gen/go/redpandadata/core/protocolbuffers/go/redpanda/core/admin/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/reflect/protoreflect"
	"gopkg.in/yaml.v2"
)

func TestShadowLinkConfigUnmarshalYAML(t *testing.T) {
	tests := []struct {
		name     string
		yamlData string
		want     ShadowLinkConfig
	}{
		{
			name: "complete config with file-based TLS",
			yamlData: `
name: "test-shadow-link"
client_options:
  bootstrap_servers:
    - "broker1:9092"
    - "broker2:9092"
  source_cluster_id: "source-123"
  tls_settings:
    enabled: true
    tls_file_settings:
      ca_path: "/path/to/ca.crt"
      key_path: "/path/to/key.pem"
      cert_path: "/path/to/cert.pem"
  authentication_configuration:
    scram_configuration:
      username: "testuser"
      password: "testpass"
      scram_mechanism: "SCRAM-SHA-256"
  metadata_max_age_ms: 10000
  connection_timeout_ms: 1000
topic_metadata_sync_options:
  interval: "30s"
  auto_create_shadow_topic_filters:
    - pattern_type: "LITERAL"
      filter_type: "INCLUDE"
      name: "test-topic"
  synced_shadow_topic_properties:
    - "retention.ms"
consumer_offset_sync_options:
  interval: "30s"
  paused: false
  group_filters:
    - pattern_type: "LITERAL"
      filter_type: "INCLUDE"
      name: "*"
security_sync_options:
  interval: "30s"
  paused: false
`,
			want: ShadowLinkConfig{
				Name: "test-shadow-link",
				ClientOptions: &ShadowLinkClientOptions{
					BootstrapServers: []string{"broker1:9092", "broker2:9092"},
					SourceClusterID:  "source-123",
					TLSSettings: &TLSSettings{
						Enabled: true,
						TLSFileSettings: &TLSFileSettings{
							CAPath:   "/path/to/ca.crt",
							KeyPath:  "/path/to/key.pem",
							CertPath: "/path/to/cert.pem",
						},
					},
					AuthenticationConfiguration: &AuthenticationConfiguration{
						ScramConfiguration: &ScramConfiguration{
							Username:       "testuser",
							Password:       "testpass",
							ScramMechanism: "SCRAM-SHA-256",
						},
					},
					MetadataMaxAgeMs:    10000,
					ConnectionTimeoutMs: 1000,
				},
				TopicMetadataSyncOptions: &TopicMetadataSyncOptions{
					Interval:       30 * time.Second,
					ExcludeDefault: false,
					AutoCreateShadowTopicFilters: []*NameFilter{
						{
							PatternType: "LITERAL",
							FilterType:  "INCLUDE",
							Name:        "test-topic",
						},
					},
					SyncedShadowTopicProperties: []string{"retention.ms"},
				},
				ConsumerOffsetSyncOptions: &ConsumerOffsetSyncOptions{
					Interval: 30 * time.Second,
					Paused:   false,
					GroupFilters: []*NameFilter{
						{
							PatternType: "LITERAL",
							FilterType:  "INCLUDE",
							Name:        "*",
						},
					},
				},
				SecuritySyncOptions: &SecuritySettingsSyncOptions{
					Interval: 30 * time.Second,
					Paused:   false,
				},
			},
		},
		{
			name: "PEM-based TLS settings",
			yamlData: `
name: "pem-test"
client_options:
  bootstrap_servers:
    - "broker1:9092"
  tls_settings:
    enabled: true
    do_not_set_sni_hostname: true
    tls_pem_settings:
      ca: |
        -----BEGIN CERTIFICATE-----
        test-ca-content
        -----END CERTIFICATE-----
      key: |
        -----BEGIN PRIVATE KEY-----
        test-key-content
        -----END PRIVATE KEY-----
      cert: |
        -----BEGIN CERTIFICATE-----
        test-cert-content
        -----END CERTIFICATE-----
  authentication_configuration:
    scram_configuration:
      username: "pemuser"
      password: "pempass"
      scram_mechanism: "SCRAM-SHA-512"
`,
			want: ShadowLinkConfig{
				Name: "pem-test",
				ClientOptions: &ShadowLinkClientOptions{
					BootstrapServers: []string{"broker1:9092"},
					TLSSettings: &TLSSettings{
						Enabled:             true,
						DoNotSetSniHostname: true,
						TLSPEMSettings: &TLSPEMSettings{
							CA:   "-----BEGIN CERTIFICATE-----\ntest-ca-content\n-----END CERTIFICATE-----\n",
							Key:  "-----BEGIN PRIVATE KEY-----\ntest-key-content\n-----END PRIVATE KEY-----\n",
							Cert: "-----BEGIN CERTIFICATE-----\ntest-cert-content\n-----END CERTIFICATE-----\n",
						},
					},
					AuthenticationConfiguration: &AuthenticationConfiguration{
						ScramConfiguration: &ScramConfiguration{
							Username:       "pemuser",
							Password:       "pempass",
							ScramMechanism: "SCRAM-SHA-512",
						},
					},
				},
			},
		},
		{
			name: "minimal config",
			yamlData: `
name: "minimal"
client_options:
  bootstrap_servers:
    - "localhost:9092"
`,
			want: ShadowLinkConfig{
				Name: "minimal",
				ClientOptions: &ShadowLinkClientOptions{
					BootstrapServers: []string{"localhost:9092"},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got ShadowLinkConfig
			err := yaml.Unmarshal([]byte(tt.yamlData), &got)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestShadowLinkConfigUnmarshalJSON(t *testing.T) {
	tests := []struct {
		name     string
		jsonData string
		want     ShadowLinkConfig
	}{
		{
			name: "complete config with file-based TLS",
			jsonData: `{
				"name": "test-shadow-link",
				"client_options": {
					"bootstrap_servers": ["broker1:9092", "broker2:9092"],
					"source_cluster_id": "source-123",
					"tls_settings": {
						"enabled": false,
						"tls_file_settings": {
							"ca_path": "/path/to/ca.crt",
							"key_path": "/path/to/key.pem",
							"cert_path": "/path/to/cert.pem"
						}
					},
					"authentication_configuration": {
						"scram_configuration": {
							"username": "testuser",
							"password": "testpass",
							"scram_mechanism": "SCRAM-SHA-256"
						}
					},
					"metadata_max_age_ms": 10000,
					"connection_timeout_ms": 1000
				},
				"topic_metadata_sync_options": {
					"exclude_default": true,
					"interval": 30000000000,
					"auto_create_shadow_topic_filters": [{
						"pattern_type": "LITERAL",
						"filter_type": "INCLUDE",
						"name": "test-topic"
					}],
					"synced_shadow_topic_properties": ["retention.ms"]
				},
				"consumer_offset_sync_options": {
					"interval": 30000000000,
					"paused": false,
					"group_filters": [{
						"pattern_type": "LITERAL",
						"filter_type": "INCLUDE",
						"name": "*"
					}]
				},
				"security_sync_options": {
					"interval": 30000000000,
					"paused": false
				}
			}`,
			want: ShadowLinkConfig{
				Name: "test-shadow-link",
				ClientOptions: &ShadowLinkClientOptions{
					BootstrapServers: []string{"broker1:9092", "broker2:9092"},
					SourceClusterID:  "source-123",
					TLSSettings: &TLSSettings{
						TLSFileSettings: &TLSFileSettings{
							CAPath:   "/path/to/ca.crt",
							KeyPath:  "/path/to/key.pem",
							CertPath: "/path/to/cert.pem",
						},
						Enabled: false,
					},
					AuthenticationConfiguration: &AuthenticationConfiguration{
						ScramConfiguration: &ScramConfiguration{
							Username:       "testuser",
							Password:       "testpass",
							ScramMechanism: "SCRAM-SHA-256",
						},
					},
					MetadataMaxAgeMs:    10000,
					ConnectionTimeoutMs: 1000,
				},
				TopicMetadataSyncOptions: &TopicMetadataSyncOptions{
					ExcludeDefault: true,
					Interval:       30 * time.Second,
					AutoCreateShadowTopicFilters: []*NameFilter{
						{
							PatternType: "LITERAL",
							FilterType:  "INCLUDE",
							Name:        "test-topic",
						},
					},
					SyncedShadowTopicProperties: []string{"retention.ms"},
				},
				ConsumerOffsetSyncOptions: &ConsumerOffsetSyncOptions{
					Interval: 30 * time.Second,
					Paused:   false,
					GroupFilters: []*NameFilter{
						{
							PatternType: "LITERAL",
							FilterType:  "INCLUDE",
							Name:        "*",
						},
					},
				},
				SecuritySyncOptions: &SecuritySettingsSyncOptions{
					Interval: 30 * time.Second,
					Paused:   false,
				},
			},
		},
		{
			name: "PEM-based TLS settings",
			jsonData: `{
				"name": "pem-test",
				"client_options": {
					"bootstrap_servers": ["broker1:9092"],
					"tls_settings": {
						"tls_pem_settings": {
							"ca": "-----BEGIN CERTIFICATE-----\ntest-ca-content\n-----END CERTIFICATE-----\n",
							"key": "-----BEGIN PRIVATE KEY-----\ntest-key-content\n-----END PRIVATE KEY-----\n",
							"cert": "-----BEGIN CERTIFICATE-----\ntest-cert-content\n-----END CERTIFICATE-----\n"
						}
					},
					"authentication_configuration": {
						"scram_configuration": {
							"username": "pemuser",
							"password": "pempass",
							"scram_mechanism": "SCRAM-SHA-512"
						}
					}
				}
			}`,
			want: ShadowLinkConfig{
				Name: "pem-test",
				ClientOptions: &ShadowLinkClientOptions{
					BootstrapServers: []string{"broker1:9092"},
					TLSSettings: &TLSSettings{
						TLSPEMSettings: &TLSPEMSettings{
							CA:   "-----BEGIN CERTIFICATE-----\ntest-ca-content\n-----END CERTIFICATE-----\n",
							Key:  "-----BEGIN PRIVATE KEY-----\ntest-key-content\n-----END PRIVATE KEY-----\n",
							Cert: "-----BEGIN CERTIFICATE-----\ntest-cert-content\n-----END CERTIFICATE-----\n",
						},
					},
					AuthenticationConfiguration: &AuthenticationConfiguration{
						ScramConfiguration: &ScramConfiguration{
							Username:       "pemuser",
							Password:       "pempass",
							ScramMechanism: "SCRAM-SHA-512",
						},
					},
				},
			},
		},
		{
			name: "minimal config",
			jsonData: `{
				"name": "minimal",
				"client_options": {
					"bootstrap_servers": ["localhost:9092"]
				}
			}`,
			want: ShadowLinkConfig{
				Name: "minimal",
				ClientOptions: &ShadowLinkClientOptions{
					BootstrapServers: []string{"localhost:9092"},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got ShadowLinkConfig
			err := json.Unmarshal([]byte(tt.jsonData), &got)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

// TestShadowLinkConfigDrift validates that ShadowLinkConfig stays in sync with
// the protobuf-generated adminv2.ShadowLinkConfigurations.
func TestShadowLinkConfigDrift(t *testing.T) {
	// This is the list of fields that we _know_ are represented differently.
	excludedPatterns := []*regexp.Regexp{
		// Intervals are represented as time.Duration but in protobuf they are
		// durationpb.Duration which has an underlying struct with exported
		// fields.
		regexp.MustCompile(`^.*\.interval$`),
		// Enums in protobuf but string in our config.
		regexp.MustCompile(`_filters\..*_type$`),
		regexp.MustCompile(`^.*\.scram_configuration.scram_mechanism$`),
		regexp.MustCompile(`^security_sync_options\.acl_filters\.access_filter\.operation$`),
		// one of: start_at_timestamp is a oneof field in protobuf but is a
		// duration. In proto, it is parsed as a struct containing a timestamp
		// field, but in the resulting JSON it is represented as a timestamp,
		// not a struct.
		regexp.MustCompile(`^topic_metadata_sync_options\.start_at_timestamp$`),
	}

	// Use protoreflect to walk the protobuf message.
	protoConfigMap := make(map[string]string)
	msgDesc := new(adminv2.ShadowLinkConfigurations).ProtoReflect().Descriptor()
	walkProtoMessage(msgDesc, "", protoConfigMap, excludedPatterns)

	// Use regular reflection to walk the Go struct.
	shadowConfigMap := make(map[string]string)
	shadowConfigType := reflect.TypeFor[ShadowLinkConfig]()
	walkType(shadowConfigType, "", shadowConfigMap, excludedPatterns)

	// Compare the two maps
	for k, v := range protoConfigMap {
		sv, ok := shadowConfigMap[k]
		if !ok {
			t.Errorf("Missing field: %q is missing in ShadowLinkConfig; was the proto updated?", k)
			continue
		}
		if sv != v {
			t.Errorf("Mismatch Type: field %q has type %q in protobuf but %q in ShadowLinkConfig", k, v, sv)
			continue
		}
	}
}

// isExcluded checks if a field matches any of the excluded patterns.
func isExcluded(field string, excludedPatterns []*regexp.Regexp) bool {
	for _, re := range excludedPatterns {
		if re.MatchString(field) {
			return true
		}
	}
	return false
}

func normalizeKindString(t reflect.Type) string {
	var kString string
	switch k := t.Kind(); k {
	case reflect.Pointer:
		kString = normalizeKindString(t.Elem())
	case reflect.Slice, reflect.Array:
		kString = fmt.Sprintf("[]%v", normalizeKindString(t.Elem()))
	case reflect.Map:
		kString = fmt.Sprintf("map[%v]%v", t.Key(), normalizeKindString(t.Elem()))
	default:
		kString = t.Kind().String()
	}
	return kString
}

// protoKindToString converts a protoreflect.Kind to a type string to match
// our normalized type strings.
func protoKindToString(kind protoreflect.Kind) string {
	switch kind {
	case protoreflect.BoolKind:
		return "bool"
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		return "int32"
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return "int64"
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return "uint32"
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return "uint64"
	case protoreflect.FloatKind:
		return "float32"
	case protoreflect.DoubleKind:
		return "float64"
	case protoreflect.StringKind:
		return "string"
	case protoreflect.BytesKind:
		return "[]uint8"
	case protoreflect.EnumKind:
		return "int32"
	case protoreflect.MessageKind:
		return "struct"
	default:
		return "unknown"
	}
}

func walkType(v reflect.Type, parentName string, m map[string]string, excludedPatterns []*regexp.Regexp) {
	for field := range v.Fields() {
		field := field
		if field.IsExported() {
			fullName := strings.Split(field.Tag.Get("json"), ",")[0]
			if parentName != "" && fullName != "" {
				fullName = parentName + "." + fullName
			}

			// This is to avoid repetition on oneof types in protobuf which are represented as
			// structs with no exported fields. And to exclude OUTPUT_ONLY fields which are
			// still present in the struct but don't matter for our config comparison.
			if excluded := isExcluded(fullName, excludedPatterns); fullName == "" || excluded {
				continue
			}
			typ := field.Type
			if typ.Kind() == reflect.Pointer {
				typ = typ.Elem()
			}

			m[fullName] = normalizeKindString(field.Type)

			// Recurse into structs and slices of structs/pointers to structs.
			if typ.Kind() == reflect.Struct {
				walkType(typ, fullName, m, excludedPatterns)
			}
			if typ.Kind() == reflect.Slice {
				elem := typ.Elem()
				if elem.Kind() != reflect.Pointer && elem.Kind() != reflect.Struct {
					// No need to recurse into slices of primitives.
					continue
				}
				if elem.Kind() == reflect.Pointer {
					elem = elem.Elem()
				}
				walkType(elem, fullName, m, excludedPatterns)
			}
		}
	}
}

// protoFieldToTypeString converts a protoreflect.FieldDescriptor to a normalized type string
// that is compatible with the output from normalizeKindString.
func protoFieldToTypeString(fd protoreflect.FieldDescriptor) string {
	if fd.IsList() {
		// Repeated field (slice)
		return fmt.Sprintf("[]%s", protoKindToString(fd.Kind()))
	}
	return protoKindToString(fd.Kind())
}

// walkProtoMessage walks a protobuf message descriptor using protoreflect and builds
// a map of field paths to type strings, similar to the walk function but using
// protoreflect API instead of regular reflection.
func walkProtoMessage(md protoreflect.MessageDescriptor, parentName string, m map[string]string, excludedPatterns []*regexp.Regexp) {
	fields := md.Fields()
	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		if isFieldOutputOnly(field) {
			continue
		}

		// Get the proto field name (snake_case) to match the Go struct json tags
		fieldName := string(field.Name())
		fullName := fieldName
		if parentName != "" {
			fullName = parentName + "." + fieldName
		}

		// Check if this field should be excluded
		if isExcluded(fullName, excludedPatterns) {
			continue
		}

		// Get the type string for this field
		typeStr := protoFieldToTypeString(field)
		m[fullName] = typeStr

		// Recurse into message types
		if field.Kind() == protoreflect.MessageKind {
			msgDesc := field.Message()
			walkProtoMessage(msgDesc, fullName, m, excludedPatterns)
		}
	}
}
