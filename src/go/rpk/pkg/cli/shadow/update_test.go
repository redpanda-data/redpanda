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

func timePtr(t time.Time) *time.Time { return &t }

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
			name: "start_at_timestamp value change",
			original: &ShadowLinkConfig{
				TopicMetadataSyncOptions: &TopicMetadataSyncOptions{
					StartAtTimestamp: timePtr(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)),
				},
			},
			updated: &ShadowLinkConfig{
				TopicMetadataSyncOptions: &TopicMetadataSyncOptions{
					StartAtTimestamp: timePtr(time.Date(2026, 6, 15, 0, 0, 0, 0, time.UTC)),
				},
			},
			want: []string{"configurations.topic_metadata_sync_options.start_at_timestamp"},
		},
		{
			// Same instant with different internal representations must not
			// produce a diff.
			name: "start_at_timestamp same instant different location",
			original: &ShadowLinkConfig{
				TopicMetadataSyncOptions: &TopicMetadataSyncOptions{
					StartAtTimestamp: timePtr(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)),
				},
			},
			updated: &ShadowLinkConfig{
				TopicMetadataSyncOptions: &TopicMetadataSyncOptions{
					StartAtTimestamp: timePtr(time.Date(2025, 1, 1, 1, 0, 0, 0, time.FixedZone("X", 3600))),
				},
			},
			want: nil,
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

func TestSelfHostedMaskPaths(t *testing.T) {
	tests := []struct {
		name string
		diff []string
		want []string
	}{
		{
			name: "scalar paths unchanged",
			diff: []string{
				"configurations.client_options.metadata_max_age_ms",
				"configurations.topic_metadata_sync_options.exclude_default",
			},
			want: []string{
				"configurations.client_options.metadata_max_age_ms",
				"configurations.topic_metadata_sync_options.exclude_default",
			},
		},
		{
			name: "message path unchanged",
			diff: []string{"configurations.security_sync_options"},
			want: []string{"configurations.security_sync_options"},
		},
		{
			name: "topic filters widened to parent",
			diff: []string{"configurations.topic_metadata_sync_options.auto_create_shadow_topic_filters"},
			want: []string{"configurations.topic_metadata_sync_options"},
		},
		{
			name: "two repeated siblings collapse to one parent",
			diff: []string{
				"configurations.topic_metadata_sync_options.auto_create_shadow_topic_filters",
				"configurations.topic_metadata_sync_options.synced_shadow_topic_properties",
			},
			want: []string{"configurations.topic_metadata_sync_options"},
		},
		{
			name: "repeated and sibling scalar collapse to parent",
			diff: []string{
				"configurations.topic_metadata_sync_options.synced_shadow_topic_properties",
				"configurations.topic_metadata_sync_options.exclude_default",
			},
			want: []string{"configurations.topic_metadata_sync_options"},
		},
		{
			name: "bootstrap servers widened, other client option collapsed",
			diff: []string{
				"configurations.client_options.bootstrap_servers",
				"configurations.client_options.metadata_max_age_ms",
			},
			want: []string{"configurations.client_options"},
		},
		{
			name: "group filters widened",
			diff: []string{"configurations.consumer_offset_sync_options.group_filters"},
			want: []string{"configurations.consumer_offset_sync_options"},
		},
		{
			name: "acl filters widened",
			diff: []string{"configurations.security_sync_options.acl_filters"},
			want: []string{"configurations.security_sync_options"},
		},
		{
			name: "deeply nested schema registry repeated widened to its own parent",
			diff: []string{"configurations.schema_registry_sync_options.shadow_schema_registry_api.source_filter.contexts"},
			want: []string{"configurations.schema_registry_sync_options.shadow_schema_registry_api.source_filter"},
		},
		{
			name: "unrelated paths kept apart",
			diff: []string{
				"configurations.security_sync_options.acl_filters",
				"configurations.client_options.metadata_max_age_ms",
			},
			want: []string{
				"configurations.client_options.metadata_max_age_ms",
				"configurations.security_sync_options",
			},
		},
		{
			name: "auth oneof switch widened to the containing message",
			diff: []string{
				"configurations.client_options.authentication_configuration.scram_configuration",
				"configurations.client_options.authentication_configuration.plain_configuration",
			},
			want: []string{"configurations.client_options.authentication_configuration"},
		},
		{
			name: "schema registry mode switch widened to the containing message",
			diff: []string{
				"configurations.schema_registry_sync_options.shadow_schema_registry_topic",
				"configurations.schema_registry_sync_options.shadow_schema_registry_api",
			},
			want: []string{"configurations.schema_registry_sync_options"},
		},
		{
			name: "tls mode switch widened to the containing message",
			diff: []string{
				"configurations.client_options.tls_settings.tls_file_settings",
				"configurations.client_options.tls_settings.tls_pem_settings",
			},
			want: []string{"configurations.client_options.tls_settings"},
		},
		{
			name: "destination mapping switch widened to the containing message",
			diff: []string{
				"configurations.schema_registry_sync_options.shadow_schema_registry_api.destination.identity",
				"configurations.schema_registry_sync_options.shadow_schema_registry_api.destination.exact",
			},
			want: []string{"configurations.schema_registry_sync_options.shadow_schema_registry_api.destination"},
		},
		{
			name: "start offset switch widened to the containing message",
			diff: []string{
				"configurations.topic_metadata_sync_options.start_at_earliest",
				"configurations.topic_metadata_sync_options.start_at_latest",
			},
			want: []string{"configurations.topic_metadata_sync_options"},
		},
		{
			name: "single oneof member removal widened to the containing message",
			diff: []string{"configurations.client_options.authentication_configuration.scram_configuration"},
			want: []string{"configurations.client_options.authentication_configuration"},
		},
		{
			name: "scalar inside a oneof member not widened",
			diff: []string{"configurations.client_options.authentication_configuration.plain_configuration.username"},
			want: []string{"configurations.client_options.authentication_configuration.plain_configuration.username"},
		},
		{
			name: "proto3 optional field (synthetic oneof) not widened",
			diff: []string{"configurations.client_options.tls_settings"},
			want: []string{"configurations.client_options.tls_settings"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, selfHostedMaskPaths(tt.diff))
		})
	}
}

func TestStripRedactedPasswords(t *testing.T) {
	cfgWithPasswords := func(scram, srBasic string) *ShadowLinkConfig {
		return &ShadowLinkConfig{
			ClientOptions: &ShadowLinkClientOptions{
				AuthenticationConfiguration: &AuthenticationConfiguration{
					ScramConfiguration: &ScramConfiguration{
						Username: "user",
						Password: scram,
					},
				},
			},
			SchemaRegistrySyncOptions: &SchemaRegistrySyncOptions{
				ShadowSchemaRegistryAPI: &ShadowSchemaRegistryAPI{
					AuthOptions: &SchemaRegistryAuthOptions{
						Basic: &HTTPBasicAuthOptions{
							Username: "sr-user",
							Password: srBasic,
						},
					},
				},
			},
		}
	}

	// The placeholder injected by addRedactedPasswordString and left
	// untouched by the user is cleared.
	original := cfgWithPasswords(redactedPassword, redactedPassword)
	updated := cfgWithPasswords(redactedPassword, redactedPassword)
	stripRedactedPasswords(original, updated)
	require.Empty(t, updated.ClientOptions.AuthenticationConfiguration.ScramConfiguration.Password)
	require.Empty(t, updated.SchemaRegistrySyncOptions.ShadowSchemaRegistryAPI.AuthOptions.Basic.Password)

	// A password the user actually changed is left untouched.
	updated = cfgWithPasswords("new-password", redactedPassword)
	stripRedactedPasswords(original, updated)
	require.Equal(t, "new-password", updated.ClientOptions.AuthenticationConfiguration.ScramConfiguration.Password)

	// A literal placeholder typed on a link without a stored password (the
	// original never had it redacted) is kept, not stripped.
	original = cfgWithPasswords("", "")
	updated = cfgWithPasswords(redactedPassword, redactedPassword)
	stripRedactedPasswords(original, updated)
	require.Equal(t, redactedPassword, updated.ClientOptions.AuthenticationConfiguration.ScramConfiguration.Password)
	require.Equal(t, redactedPassword, updated.SchemaRegistrySyncOptions.ShadowSchemaRegistryAPI.AuthOptions.Basic.Password)
}

func TestCheckPlainPasswordErased(t *testing.T) {
	plainCfg := func(password string) *ShadowLinkConfig {
		return &ShadowLinkConfig{
			ClientOptions: &ShadowLinkClientOptions{
				AuthenticationConfiguration: &AuthenticationConfiguration{
					PlainConfiguration: &PlainConfiguration{
						Username: "user",
						Password: password,
					},
				},
			},
		}
	}
	tests := []struct {
		name             string
		updated          *ShadowLinkConfig
		plainPasswordSet bool
		wantErr          bool
	}{
		{
			// The cluster rebuilds the whole link on every update and cannot
			// preserve a stored PLAIN password, so the guard fires no matter
			// which fields changed.
			name:             "stored plain password not re-supplied",
			updated:          plainCfg(""),
			plainPasswordSet: true,
			wantErr:          true,
		},
		{
			name:             "plain password provided",
			updated:          plainCfg("secret"),
			plainPasswordSet: true,
		},
		{
			name:             "no plain password stored on the cluster",
			updated:          plainCfg(""),
			plainPasswordSet: false,
		},
		{
			name: "plain auth removed on purpose",
			updated: &ShadowLinkConfig{
				ClientOptions: &ShadowLinkClientOptions{
					AuthenticationConfiguration: &AuthenticationConfiguration{
						ScramConfiguration: &ScramConfiguration{Username: "user", Password: "pass"},
					},
				},
			},
			plainPasswordSet: true,
		},
		{
			name:             "client options removed entirely",
			updated:          &ShadowLinkConfig{},
			plainPasswordSet: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := checkPlainPasswordErased(tt.updated, tt.plainPasswordSet)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
