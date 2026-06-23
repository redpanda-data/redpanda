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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDiffConfigs(t *testing.T) {
	tests := []struct {
		name     string
		original *ShadowLinkConfig
		updated  *ShadowLinkConfig
		want     []string
	}{
		{
			name:     "both nil",
			original: nil,
			updated:  nil,
			want:     nil,
		},
		{
			name:     "original nil",
			original: nil,
			updated:  &ShadowLinkConfig{Name: "test"},
			want:     []string{"configurations"},
		},
		{
			name:     "updated nil",
			original: &ShadowLinkConfig{Name: "test"},
			updated:  nil,
			want:     []string{"configurations"},
		},
		{
			name:     "no changes",
			original: &ShadowLinkConfig{Name: "test"},
			updated:  &ShadowLinkConfig{Name: "test"},
			want:     nil,
		},
		{
			name:     "simple field change - name",
			original: &ShadowLinkConfig{Name: "old"},
			updated:  &ShadowLinkConfig{Name: "new"},
			want:     []string{"configurations.name"},
		},
		{
			name: "nested field change - bootstrap_servers",
			original: &ShadowLinkConfig{
				ClientOptions: &ShadowLinkClientOptions{
					BootstrapServers: []string{"host1:9092"},
				},
			},
			updated: &ShadowLinkConfig{
				ClientOptions: &ShadowLinkClientOptions{
					BootstrapServers: []string{"host2:9092"},
				},
			},
			want: []string{"configurations.client_options.bootstrap_servers"},
		},
		{
			name: "deeply nested field change - tls enabled",
			original: &ShadowLinkConfig{
				ClientOptions: &ShadowLinkClientOptions{
					TLSSettings: &TLSSettings{
						Enabled: false,
					},
				},
			},
			updated: &ShadowLinkConfig{
				ClientOptions: &ShadowLinkClientOptions{
					TLSSettings: &TLSSettings{
						Enabled: true,
					},
				},
			},
			want: []string{"configurations.client_options.tls_settings.enabled"},
		},
		{
			name: "slice length change",
			original: &ShadowLinkConfig{
				ClientOptions: &ShadowLinkClientOptions{
					BootstrapServers: []string{"host1:9092"},
				},
			},
			updated: &ShadowLinkConfig{
				ClientOptions: &ShadowLinkClientOptions{
					BootstrapServers: []string{"host1:9092", "host2:9092"},
				},
			},
			want: []string{"configurations.client_options.bootstrap_servers"},
		},
		{
			name: "slice content change",
			original: &ShadowLinkConfig{
				TopicMetadataSyncOptions: &TopicMetadataSyncOptions{
					SyncedShadowTopicProperties: []string{"retention.ms"},
				},
			},
			updated: &ShadowLinkConfig{
				TopicMetadataSyncOptions: &TopicMetadataSyncOptions{
					SyncedShadowTopicProperties: []string{"segment.ms"},
				},
			},
			want: []string{"configurations.topic_metadata_sync_options.synced_shadow_topic_properties"},
		},
		{
			name: "struct slice change - filters",
			original: &ShadowLinkConfig{
				TopicMetadataSyncOptions: &TopicMetadataSyncOptions{
					AutoCreateShadowTopicFilters: []*NameFilter{
						{PatternType: PatternTypeLiteral, FilterType: FilterTypeInclude, Name: "test"},
					},
				},
			},
			updated: &ShadowLinkConfig{
				TopicMetadataSyncOptions: &TopicMetadataSyncOptions{
					AutoCreateShadowTopicFilters: []*NameFilter{
						{PatternType: PatternTypePrefix, FilterType: FilterTypeExclude, Name: "test"},
					},
				},
			},
			want: []string{"configurations.topic_metadata_sync_options.auto_create_shadow_topic_filters"},
		},
		{
			name: "nil to non-nil pointer change",
			original: &ShadowLinkConfig{
				SecuritySyncOptions: nil,
			},
			updated: &ShadowLinkConfig{
				SecuritySyncOptions: &SecuritySettingsSyncOptions{
					Interval: 30 * time.Second,
				},
			},
			want: []string{"configurations.security_sync_options"},
		},
		{
			name: "non-nil to nil pointer change",
			original: &ShadowLinkConfig{
				SecuritySyncOptions: &SecuritySettingsSyncOptions{
					Interval: 30 * time.Second,
				},
			},
			updated: &ShadowLinkConfig{
				SecuritySyncOptions: nil,
			},
			want: []string{"configurations.security_sync_options"},
		},
		{
			name: "empty struct oneof marker change - start_at_earliest",
			original: &ShadowLinkConfig{
				TopicMetadataSyncOptions: &TopicMetadataSyncOptions{
					StartAtEarliest: nil,
				},
			},
			updated: &ShadowLinkConfig{
				TopicMetadataSyncOptions: &TopicMetadataSyncOptions{
					StartAtEarliest: &StartAtEarliest{},
				},
			},
			want: []string{"configurations.topic_metadata_sync_options.start_at_earliest"},
		},
		{
			name: "time.Duration change",
			original: &ShadowLinkConfig{
				TopicMetadataSyncOptions: &TopicMetadataSyncOptions{
					Interval: 30 * time.Second,
				},
			},
			updated: &ShadowLinkConfig{
				TopicMetadataSyncOptions: &TopicMetadataSyncOptions{
					Interval: 60 * time.Second,
				},
			},
			want: []string{"configurations.topic_metadata_sync_options.interval"},
		},
		{
			name: "int32 field change",
			original: &ShadowLinkConfig{
				ClientOptions: &ShadowLinkClientOptions{
					MetadataMaxAgeMs: 10000,
				},
			},
			updated: &ShadowLinkConfig{
				ClientOptions: &ShadowLinkClientOptions{
					MetadataMaxAgeMs: 20000,
				},
			},
			want: []string{"configurations.client_options.metadata_max_age_ms"},
		},
		{
			name: "multiple field changes",
			original: &ShadowLinkConfig{
				Name: "old",
				ClientOptions: &ShadowLinkClientOptions{
					BootstrapServers: []string{"host1:9092"},
					SourceClusterID:  "cluster1",
				},
			},
			updated: &ShadowLinkConfig{
				Name: "new",
				ClientOptions: &ShadowLinkClientOptions{
					BootstrapServers: []string{"host2:9092"},
					SourceClusterID:  "cluster2",
				},
			},
			want: []string{
				"configurations.name",
				"configurations.client_options.bootstrap_servers",
				"configurations.client_options.source_cluster_id",
			},
		},
		{
			name: "union field change - switching tls modes",
			original: &ShadowLinkConfig{
				ClientOptions: &ShadowLinkClientOptions{
					TLSSettings: &TLSSettings{
						TLSFileSettings: &TLSFileSettings{
							CAPath: "/path/ca.crt",
						},
					},
				},
			},
			updated: &ShadowLinkConfig{
				ClientOptions: &ShadowLinkClientOptions{
					TLSSettings: &TLSSettings{
						TLSPEMSettings: &TLSPEMSettings{
							CA: "-----BEGIN CERTIFICATE-----",
						},
					},
				},
			},
			want: []string{
				"configurations.client_options.tls_settings.tls_file_settings",
				"configurations.client_options.tls_settings.tls_pem_settings",
			},
		},
		{
			name: "union field change - scram config",
			original: &ShadowLinkConfig{
				ClientOptions: &ShadowLinkClientOptions{
					AuthenticationConfiguration: nil,
				},
			},
			updated: &ShadowLinkConfig{
				ClientOptions: &ShadowLinkClientOptions{
					AuthenticationConfiguration: &AuthenticationConfiguration{
						ScramConfiguration: &ScramConfiguration{
							Username:       "user",
							Password:       "pass",
							ScramMechanism: ScramMechanismScramSha256,
						},
					},
				},
			},
			want: []string{
				"configurations.client_options.authentication_configuration",
			},
		},
		{
			name: "schema registry API scalar field change - source_url",
			original: &ShadowLinkConfig{
				SchemaRegistrySyncOptions: &SchemaRegistrySyncOptions{
					ShadowSchemaRegistryAPI: &ShadowSchemaRegistryAPI{
						SourceURL: "https://old-sr:8081",
					},
				},
			},
			updated: &ShadowLinkConfig{
				SchemaRegistrySyncOptions: &SchemaRegistrySyncOptions{
					ShadowSchemaRegistryAPI: &ShadowSchemaRegistryAPI{
						SourceURL: "https://new-sr:8081",
					},
				},
			},
			want: []string{
				"configurations.schema_registry_sync_options.shadow_schema_registry_api.source_url",
			},
		},
		{
			name: "schema registry shadowing mode switch - topic to api",
			original: &ShadowLinkConfig{
				SchemaRegistrySyncOptions: &SchemaRegistrySyncOptions{
					ShadowSchemaRegistryTopic: &ShadowSchemaRegistryTopic{},
				},
			},
			updated: &ShadowLinkConfig{
				SchemaRegistrySyncOptions: &SchemaRegistrySyncOptions{
					ShadowSchemaRegistryAPI: &ShadowSchemaRegistryAPI{
						SourceURL: "https://source-sr:8081",
					},
				},
			},
			want: []string{
				"configurations.schema_registry_sync_options.shadow_schema_registry_topic",
				"configurations.schema_registry_sync_options.shadow_schema_registry_api",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := diffConfigs(tt.original, tt.updated)

			require.Equal(t, tt.want, got)
		})
	}
}
