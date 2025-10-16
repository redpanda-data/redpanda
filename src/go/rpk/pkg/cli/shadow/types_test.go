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
	"gopkg.in/yaml.v2"
)

func TestShadowLinkConfigUnmarshalYAML(t *testing.T) {
	tests := []struct {
		name     string
		yamlData string
		want     ShadowLinkConfig
		wantErr  bool
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
    ca_path: "/path/to/ca.crt"
    key_path: "/path/to/key.pem"
    cert_path: "/path/to/cert.pem"
  authentication_configuration:
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
  enabled: true
  group_filters:
    - pattern_type: "LITERAL"
      filter_type: "INCLUDE"
      name: "*"
security_sync_options:
  interval: "30s"
  enabled: true
`,
			want: ShadowLinkConfig{
				Name: "test-shadow-link",
				ClientOptions: &ShadowLinkClientOptions{
					BootstrapServers: []string{"broker1:9092", "broker2:9092"},
					SourceClusterID:  "source-123",
					TLSSettings: &TLSFileSettings{
						Enabled:  true,
						CAPath:   "/path/to/ca.crt",
						KeyPath:  "/path/to/key.pem",
						CertPath: "/path/to/cert.pem",
					},
					AuthenticationConfiguration: &ScramConfig{
						Username:       "testuser",
						Password:       "testpass",
						ScramMechanism: "SCRAM-SHA-256",
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
					Enabled:  true,
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
					Enabled:  true,
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
    username: "pemuser"
    password: "pempass"
    scram_mechanism: "SCRAM-SHA-512"
`,
			want: ShadowLinkConfig{
				Name: "pem-test",
				ClientOptions: &ShadowLinkClientOptions{
					BootstrapServers: []string{"broker1:9092"},
					TLSSettings: &TLSPEMSettings{
						Enabled: true,
						CA:      "-----BEGIN CERTIFICATE-----\ntest-ca-content\n-----END CERTIFICATE-----\n",
						Key:     "-----BEGIN PRIVATE KEY-----\ntest-key-content\n-----END PRIVATE KEY-----\n",
						Cert:    "-----BEGIN CERTIFICATE-----\ntest-cert-content\n-----END CERTIFICATE-----\n",
					},
					AuthenticationConfiguration: &ScramConfig{
						Username:       "pemuser",
						Password:       "pempass",
						ScramMechanism: "SCRAM-SHA-512",
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
		{
			name: "invalid TLS - both ca_path and ca",
			yamlData: `
name: "invalid-tls"
client_options:
  bootstrap_servers:
    - "broker1:9092"
  tls_settings:
    ca_path: "/path/to/ca.crt"
    ca: "cert-content"
`,
			wantErr: true,
		},
		{
			name: "invalid TLS - neither ca_path nor ca",
			yamlData: `
name: "invalid-tls2"
client_options:
  bootstrap_servers:
    - "broker1:9092"
  tls_settings:
    key_path: "/path/to/key.pem"
`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got ShadowLinkConfig
			err := yaml.Unmarshal([]byte(tt.yamlData), &got)

			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestTLSSettingsUnmarshalYAML(t *testing.T) {
	tests := []struct {
		name     string
		yamlData string
		want     TLSSettings
		wantErr  bool
	}{
		{
			name:     "just enabled TLS",
			yamlData: "enabled: true",
			want: &TLSPEMSettings{
				Enabled: true,
			},
		},
		{
			name: "file-based TLS settings",
			yamlData: `
ca_path: "/path/to/ca.crt"
key_path: "/path/to/key.pem"
cert_path: "/path/to/cert.pem"
`,
			want: &TLSFileSettings{
				CAPath:   "/path/to/ca.crt",
				KeyPath:  "/path/to/key.pem",
				CertPath: "/path/to/cert.pem",
			},
		},
		{
			name: "PEM-based TLS settings",
			yamlData: `
ca: "ca-content"
key: "key-content"
cert: "cert-content"
`,
			want: &TLSPEMSettings{
				CA:   "ca-content",
				Key:  "key-content",
				Cert: "cert-content",
			},
		},
		{
			name: "error - both ca_path and ca",
			yamlData: `
ca_path: "/path/to/ca.crt"
ca: "ca-content"
`,
			wantErr: true,
		},
		{
			name: "error - neither ca_path nor ca (enabled is false)",
			yamlData: `
enabled: false
key_path: "/path/to/key.pem"
`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var wrapper tlsSettingsWrapper
			err := yaml.Unmarshal([]byte(tt.yamlData), &wrapper)

			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.want, wrapper.TLSSettings)
		})
	}
}

func TestAuthenticationConfigurationUnmarshalYAML(t *testing.T) {
	tests := []struct {
		name     string
		yamlData string
		want     AuthenticationConfiguration
		wantErr  bool
	}{
		{
			name: "SCRAM authentication",
			yamlData: `
username: "testuser"
password: "testpass"
scram_mechanism: "SCRAM-SHA-256"
`,
			want: &ScramConfig{
				Username:       "testuser",
				Password:       "testpass",
				ScramMechanism: "SCRAM-SHA-256",
			},
		},
		{
			name: "SCRAM SHA-512",
			yamlData: `
username: "testuser2"
password: "testpass2"
scram_mechanism: "SCRAM-SHA-512"
`,
			want: &ScramConfig{
				Username:       "testuser2",
				Password:       "testpass2",
				ScramMechanism: "SCRAM-SHA-512",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var wrapper authConfigWrapper
			err := yaml.Unmarshal([]byte(tt.yamlData), &wrapper)

			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.want, wrapper.AuthenticationConfiguration)
		})
	}
}

func TestShadowLinkConfigUnmarshalJSON(t *testing.T) {
	tests := []struct {
		name     string
		jsonData string
		want     ShadowLinkConfig
		wantErr  bool
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
						"ca_path": "/path/to/ca.crt",
						"key_path": "/path/to/key.pem",
						"cert_path": "/path/to/cert.pem"
					},
					"authentication_configuration": {
						"username": "testuser",
						"password": "testpass",
						"scram_mechanism": "SCRAM-SHA-256"
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
					"enabled": true,
					"group_filters": [{
						"pattern_type": "LITERAL",
						"filter_type": "INCLUDE",
						"name": "*"
					}]
				},
				"security_sync_options": {
					"interval": 30000000000,
					"enabled": true
				}
			}`,
			want: ShadowLinkConfig{
				Name: "test-shadow-link",
				ClientOptions: &ShadowLinkClientOptions{
					BootstrapServers: []string{"broker1:9092", "broker2:9092"},
					SourceClusterID:  "source-123",
					TLSSettings: &TLSFileSettings{
						Enabled:  false,
						CAPath:   "/path/to/ca.crt",
						KeyPath:  "/path/to/key.pem",
						CertPath: "/path/to/cert.pem",
					},
					AuthenticationConfiguration: &ScramConfig{
						Username:       "testuser",
						Password:       "testpass",
						ScramMechanism: "SCRAM-SHA-256",
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
					Enabled:  true,
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
					Enabled:  true,
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
						"ca": "-----BEGIN CERTIFICATE-----\ntest-ca-content\n-----END CERTIFICATE-----\n",
						"key": "-----BEGIN PRIVATE KEY-----\ntest-key-content\n-----END PRIVATE KEY-----\n",
						"cert": "-----BEGIN CERTIFICATE-----\ntest-cert-content\n-----END CERTIFICATE-----\n"
					},
					"authentication_configuration": {
						"username": "pemuser",
						"password": "pempass",
						"scram_mechanism": "SCRAM-SHA-512"
					}
				}
			}`,
			want: ShadowLinkConfig{
				Name: "pem-test",
				ClientOptions: &ShadowLinkClientOptions{
					BootstrapServers: []string{"broker1:9092"},
					TLSSettings: &TLSPEMSettings{
						CA:   "-----BEGIN CERTIFICATE-----\ntest-ca-content\n-----END CERTIFICATE-----\n",
						Key:  "-----BEGIN PRIVATE KEY-----\ntest-key-content\n-----END PRIVATE KEY-----\n",
						Cert: "-----BEGIN CERTIFICATE-----\ntest-cert-content\n-----END CERTIFICATE-----\n",
					},
					AuthenticationConfiguration: &ScramConfig{
						Username:       "pemuser",
						Password:       "pempass",
						ScramMechanism: "SCRAM-SHA-512",
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
		{
			name: "invalid TLS - both ca_path and ca",
			jsonData: `{
				"name": "invalid-tls",
				"client_options": {
					"bootstrap_servers": ["broker1:9092"],
					"tls_settings": {
						"ca_path": "/path/to/ca.crt",
						"ca": "cert-content"
					}
				}
			}`,
			wantErr: true,
		},
		{
			name: "invalid TLS - neither ca_path nor ca",
			jsonData: `{
				"name": "invalid-tls2",
				"client_options": {
					"bootstrap_servers": ["broker1:9092"],
					"tls_settings": {
						"key_path": "/path/to/key.pem"
					}
				}
			}`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got ShadowLinkConfig
			err := json.Unmarshal([]byte(tt.jsonData), &got)

			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestTLSSettingsUnmarshalJSON(t *testing.T) {
	tests := []struct {
		name     string
		jsonData string
		want     TLSSettings
		wantErr  bool
	}{
		{
			name:     "just enabled TLS",
			jsonData: `{"enabled": true}`,
			want: &TLSPEMSettings{
				Enabled: true,
			},
		},
		{
			name: "file-based TLS settings",
			jsonData: `{
				"enabled": true,
				"ca_path": "/path/to/ca.crt",
				"key_path": "/path/to/key.pem",
				"cert_path": "/path/to/cert.pem"
			}`,
			want: &TLSFileSettings{
				Enabled:  true,
				CAPath:   "/path/to/ca.crt",
				KeyPath:  "/path/to/key.pem",
				CertPath: "/path/to/cert.pem",
			},
		},
		{
			name: "PEM-based TLS settings",
			jsonData: `{
				"enabled": false,
				"ca": "ca-content",
				"key": "key-content",
				"cert": "cert-content"
			}`,
			want: &TLSPEMSettings{
				Enabled: false,
				CA:      "ca-content",
				Key:     "key-content",
				Cert:    "cert-content",
			},
		},
		{
			name: "error - both ca_path and ca",
			jsonData: `{
				"ca_path": "/path/to/ca.crt",
				"ca": "ca-content"
			}`,
			wantErr: true,
		},
		{
			name: "error - neither ca_path nor ca and enabled is false",
			jsonData: `{
				"enabled": false,
				"key_path": "/path/to/key.pem"
			}`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var wrapper tlsSettingsWrapper
			err := json.Unmarshal([]byte(tt.jsonData), &wrapper)

			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.want, wrapper.TLSSettings)
		})
	}
}

func TestAuthenticationConfigurationUnmarshalJSON(t *testing.T) {
	tests := []struct {
		name     string
		jsonData string
		want     AuthenticationConfiguration
		wantErr  bool
	}{
		{
			name: "SCRAM authentication",
			jsonData: `{
				"username": "testuser",
				"password": "testpass",
				"scram_mechanism": "SCRAM-SHA-256"
			}`,
			want: &ScramConfig{
				Username:       "testuser",
				Password:       "testpass",
				ScramMechanism: "SCRAM-SHA-256",
			},
		},
		{
			name: "SCRAM SHA-512",
			jsonData: `{
				"username": "testuser2",
				"password": "testpass2",
				"scram_mechanism": "SCRAM-SHA-512"
			}`,
			want: &ScramConfig{
				Username:       "testuser2",
				Password:       "testpass2",
				ScramMechanism: "SCRAM-SHA-512",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var wrapper authConfigWrapper
			err := json.Unmarshal([]byte(tt.jsonData), &wrapper)

			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.want, wrapper.AuthenticationConfiguration)
		})
	}
}

// TestShadowLinkConfigChanges validates that ShadowLinkConfig stays in sync
// with the protobuf-generated adminv2.ShadowLinkConfigurations structure.
func TestShadowLinkConfigChanges(t *testing.T) {
	excludedPatterns := []*regexp.Regexp{
		// OUTPUT_ONLY fields, we don't care about them in our configuration.
		// We do an exact match.
		regexp.MustCompile(`^client_options\.client_id$`),
		// Intervals are represented as time.Duration but in protobuf they are
		// durationpb.Duration which has an underlying struct with exported
		// fields.
		regexp.MustCompile(`^.*\.interval$`),
		// Filter types are enums in protobuf, but strings in our config.
		regexp.MustCompile(`_filters\..*_type$`),
		// Special-case exclusion for exactly this one field which is also an
		// enum.
		regexp.MustCompile(`^security_sync_options\.acl_filters\.access_filter\.operation$`),
		// A one-of type in protobuf, represented as an interface{} in Go.
		regexp.MustCompile(`client_options.tls_settings.enabled$`),
	}

	var walk func(v reflect.Type, parentName string, m map[string]string)
	walk = func(v reflect.Type, parentName string, m map[string]string) {
		for i := 0; i < v.NumField(); i++ {
			field := v.Field(i)
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
					walk(typ, fullName, m)
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
					walk(elem, fullName, m)
				}
			}
		}
	}

	// It's a flat map of field:type
	adminConfigMap := make(map[string]string)
	adminType := reflect.TypeOf(adminv2.ShadowLinkConfigurations{})
	walk(adminType, "", adminConfigMap)

	shadowConfigMap := make(map[string]string)
	shadowConfigType := reflect.TypeOf(ShadowLinkConfig{})
	walk(shadowConfigType, "", shadowConfigMap)

	for k, v := range adminConfigMap {
		sv, ok := shadowConfigMap[k]
		if !ok {
			t.Errorf("Missing field: %q is missing in ShadowLinkConfig; was the proto updated? is it OUTPUT_ONLY?", k)
			continue
		}
		if sv != v {
			t.Errorf("Mismatch Type: field %q has type %q in protobuf but %q in ShadowLinkConfig", k, v, sv)
			continue
		}
	}
}

// This is our opinionated normalization of type strings to avoid false
// positives and deal with some differences in representation between protobuf
// and our selected Go types.
func normalizeKindString(t reflect.Type) string {
	var kString string
	switch k := t.Kind(); k {
	case reflect.Interface:
		// Opinionated: we use an interface to represent a union type
		// (oneof in protobuf), while protobuf generates a struct.
		return "struct"
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

// isExcluded checks if a field matches any of the excluded patterns.
func isExcluded(field string, excludedPatterns []*regexp.Regexp) bool {
	for _, re := range excludedPatterns {
		if re.MatchString(field) {
			return true
		}
	}
	return false
}
