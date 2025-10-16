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

	adminv2 "buf.build/gen/go/redpandadata/core/protocolbuffers/go/redpanda/core/admin/v2"
	"buf.build/gen/go/redpandadata/core/protocolbuffers/go/redpanda/core/common"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestShadowLinkConfigToCreateReq(t *testing.T) {
	tests := []struct {
		name string
		cfg  *ShadowLinkConfig
		want *adminv2.CreateShadowLinkRequest
	}{
		{
			name: "nil config returns nil",
			cfg:  nil,
			want: nil,
		},
		{
			name: "minimal config",
			cfg: &ShadowLinkConfig{
				Name: "test-link",
			},
			want: &adminv2.CreateShadowLinkRequest{
				ShadowLink: &adminv2.ShadowLink{
					Name:           "test-link",
					Configurations: &adminv2.ShadowLinkConfigurations{},
				},
			},
		},
		{
			name: "complete config with all options",
			cfg: &ShadowLinkConfig{
				Name: "complete-link",
				ClientOptions: &ShadowLinkClientOptions{
					BootstrapServers:    []string{"broker1:9092", "broker2:9092"},
					SourceClusterID:     "cluster-123",
					MetadataMaxAgeMs:    10000,
					ConnectionTimeoutMs: 1000,
					RetryBackoffMs:      100,
					FetchWaitMaxMs:      500,
					FetchMinBytes:       1,
					FetchMaxBytes:       1048576,
					TLSSettings: &TLSFileSettings{
						Enabled:  true,
						CAPath:   "/path/to/ca.crt",
						KeyPath:  "/path/to/key.pem",
						CertPath: "/path/to/cert.pem",
					},
					AuthenticationConfiguration: &ScramConfig{
						Username:       "testuser",
						Password:       "testpass",
						ScramMechanism: ScramMechanismScramSha256,
					},
				},
				TopicMetadataSyncOptions: &TopicMetadataSyncOptions{
					Interval: 30 * time.Second,
					AutoCreateShadowTopicFilters: []*NameFilter{
						{
							PatternType: PatternTypeLiteral,
							FilterType:  FilterTypeInclude,
							Name:        "test-topic",
						},
					},
					SyncedShadowTopicProperties: []string{"retention.ms"},
					ExcludeDefault:              true,
				},
				ConsumerOffsetSyncOptions: &ConsumerOffsetSyncOptions{
					Enabled:  true,
					Interval: 30 * time.Second,
					GroupFilters: []*NameFilter{
						{
							PatternType: PatternTypePrefix,
							FilterType:  FilterTypeExclude,
							Name:        "test-",
						},
					},
				},
				SecuritySyncOptions: &SecuritySettingsSyncOptions{
					Enabled:  true,
					Interval: 30 * time.Second,
					ACLFilters: []*ACLFilter{
						{
							ResourceFilter: &ACLResourceFilter{
								ResourceType: ACLResourceTopic,
								PatternType:  ACLPatternLiteral,
								Name:         "secure-topic",
							},
							AccessFilter: &ACLAccessFilter{
								Principal:      "User:alice",
								Operation:      ACLOperationRead,
								PermissionType: ACLPermissionTypeAllow,
								Host:           "*",
							},
						},
					},
				},
			},
			want: &adminv2.CreateShadowLinkRequest{
				ShadowLink: &adminv2.ShadowLink{
					Name: "complete-link",
					Configurations: &adminv2.ShadowLinkConfigurations{
						ClientOptions: &adminv2.ShadowLinkClientOptions{
							BootstrapServers:    []string{"broker1:9092", "broker2:9092"},
							SourceClusterId:     "cluster-123",
							MetadataMaxAgeMs:    10000,
							ConnectionTimeoutMs: 1000,
							RetryBackoffMs:      100,
							FetchWaitMaxMs:      500,
							FetchMinBytes:       1,
							FetchMaxBytes:       1048576,
							TlsSettings: &adminv2.TLSSettings{
								Enabled: true,
								TlsSettings: &adminv2.TLSSettings_TlsFileSettings{
									TlsFileSettings: &adminv2.TLSFileSettings{
										CaPath:   "/path/to/ca.crt",
										KeyPath:  "/path/to/key.pem",
										CertPath: "/path/to/cert.pem",
									},
								},
							},
							AuthenticationConfiguration: &adminv2.AuthenticationConfiguration{
								Authentication: &adminv2.AuthenticationConfiguration_ScramConfiguration{
									ScramConfiguration: &adminv2.ScramConfig{
										Username:       "testuser",
										Password:       "testpass",
										ScramMechanism: adminv2.ScramMechanism_SCRAM_MECHANISM_SCRAM_SHA_256,
									},
								},
							},
						},
						TopicMetadataSyncOptions: &adminv2.TopicMetadataSyncOptions{
							Interval: durationpb.New(30 * time.Second),
							AutoCreateShadowTopicFilters: []*adminv2.NameFilter{
								{
									PatternType: adminv2.PatternType_PATTERN_TYPE_LITERAL,
									FilterType:  adminv2.FilterType_FILTER_TYPE_INCLUDE,
									Name:        "test-topic",
								},
							},
							SyncedShadowTopicProperties: []string{"retention.ms"},
							ExcludeDefault:              true,
						},
						ConsumerOffsetSyncOptions: &adminv2.ConsumerOffsetSyncOptions{
							Enabled:  true,
							Interval: durationpb.New(30 * time.Second),
							GroupFilters: []*adminv2.NameFilter{
								{
									PatternType: adminv2.PatternType_PATTERN_TYPE_PREFIX,
									FilterType:  adminv2.FilterType_FILTER_TYPE_EXCLUDE,
									Name:        "test-",
								},
							},
						},
						SecuritySyncOptions: &adminv2.SecuritySettingsSyncOptions{
							Enabled:  true,
							Interval: durationpb.New(30 * time.Second),
							AclFilters: []*adminv2.ACLFilter{
								{
									ResourceFilter: &adminv2.ACLResourceFilter{
										ResourceType: common.ACLResource_ACL_RESOURCE_TOPIC,
										PatternType:  common.ACLPattern_ACL_PATTERN_LITERAL,
										Name:         "secure-topic",
									},
									AccessFilter: &adminv2.ACLAccessFilter{
										Principal:      "User:alice",
										Operation:      common.ACLOperation_ACL_OPERATION_READ,
										PermissionType: common.ACLPermissionType_ACL_PERMISSION_TYPE_ALLOW,
										Host:           "*",
									},
								},
							},
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := shadowLinkConfigToCreateReq(tt.cfg)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestMapClientOptions(t *testing.T) {
	tests := []struct {
		name string
		opts *ShadowLinkClientOptions
		want *adminv2.ShadowLinkClientOptions
	}{
		{
			name: "nil options returns nil",
			opts: nil,
			want: nil,
		},
		{
			name: "basic options without TLS or auth",
			opts: &ShadowLinkClientOptions{
				BootstrapServers:    []string{"localhost:9092"},
				SourceClusterID:     "test-cluster",
				MetadataMaxAgeMs:    5000,
				ConnectionTimeoutMs: 2000,
				RetryBackoffMs:      200,
				FetchWaitMaxMs:      1000,
				FetchMinBytes:       10,
				FetchMaxBytes:       2097152,
			},
			want: &adminv2.ShadowLinkClientOptions{
				BootstrapServers:    []string{"localhost:9092"},
				SourceClusterId:     "test-cluster",
				MetadataMaxAgeMs:    5000,
				ConnectionTimeoutMs: 2000,
				RetryBackoffMs:      200,
				FetchWaitMaxMs:      1000,
				FetchMinBytes:       10,
				FetchMaxBytes:       2097152,
			},
		},
		{
			name: "with file-based TLS",
			opts: &ShadowLinkClientOptions{
				BootstrapServers: []string{"localhost:9092"},
				TLSSettings: &TLSFileSettings{
					Enabled:  true,
					CAPath:   "/ca.crt",
					KeyPath:  "/key.pem",
					CertPath: "/cert.pem",
				},
			},
			want: &adminv2.ShadowLinkClientOptions{
				BootstrapServers: []string{"localhost:9092"},
				TlsSettings: &adminv2.TLSSettings{
					Enabled: true,
					TlsSettings: &adminv2.TLSSettings_TlsFileSettings{
						TlsFileSettings: &adminv2.TLSFileSettings{
							CaPath:   "/ca.crt",
							KeyPath:  "/key.pem",
							CertPath: "/cert.pem",
						},
					},
				},
			},
		},
		{
			name: "with PEM-based TLS",
			opts: &ShadowLinkClientOptions{
				BootstrapServers: []string{"localhost:9092"},
				TLSSettings: &TLSPEMSettings{
					Enabled: true,
					CA:      "ca-content",
					Key:     "key-content",
					Cert:    "cert-content",
				},
			},
			want: &adminv2.ShadowLinkClientOptions{
				BootstrapServers: []string{"localhost:9092"},
				TlsSettings: &adminv2.TLSSettings{
					Enabled: true,
					TlsSettings: &adminv2.TLSSettings_TlsPemSettings{
						TlsPemSettings: &adminv2.TLSPEMSettings{
							Ca:   "ca-content",
							Key:  "key-content",
							Cert: "cert-content",
						},
					},
				},
			},
		},
		{
			name: "with SCRAM authentication",
			opts: &ShadowLinkClientOptions{
				BootstrapServers: []string{"localhost:9092"},
				AuthenticationConfiguration: &ScramConfig{
					Username:       "user",
					Password:       "pass",
					ScramMechanism: ScramMechanismScramSha512,
				},
			},
			want: &adminv2.ShadowLinkClientOptions{
				BootstrapServers: []string{"localhost:9092"},
				AuthenticationConfiguration: &adminv2.AuthenticationConfiguration{
					Authentication: &adminv2.AuthenticationConfiguration_ScramConfiguration{
						ScramConfiguration: &adminv2.ScramConfig{
							Username:       "user",
							Password:       "pass",
							ScramMechanism: adminv2.ScramMechanism_SCRAM_MECHANISM_SCRAM_SHA_512,
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mapClientOptions(tt.opts)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestMapTLSSettings(t *testing.T) {
	tests := []struct {
		name string
		tls  TLSSettings
		want *adminv2.TLSSettings
	}{
		{
			name: "nil TLS returns nil",
			tls:  nil,
			want: nil,
		},
		{
			name: "file-based TLS settings",
			tls: &TLSFileSettings{
				Enabled:  true,
				CAPath:   "/path/to/ca",
				KeyPath:  "/path/to/key",
				CertPath: "/path/to/cert",
			},
			want: &adminv2.TLSSettings{
				Enabled: true,
				TlsSettings: &adminv2.TLSSettings_TlsFileSettings{
					TlsFileSettings: &adminv2.TLSFileSettings{
						CaPath:   "/path/to/ca",
						KeyPath:  "/path/to/key",
						CertPath: "/path/to/cert",
					},
				},
			},
		},
		{
			name: "PEM-based TLS settings",
			tls: &TLSPEMSettings{
				Enabled: false,
				CA:      "ca-pem",
				Key:     "key-pem",
				Cert:    "cert-pem",
			},
			want: &adminv2.TLSSettings{
				Enabled: false,
				TlsSettings: &adminv2.TLSSettings_TlsPemSettings{
					TlsPemSettings: &adminv2.TLSPEMSettings{
						Ca:   "ca-pem",
						Key:  "key-pem",
						Cert: "cert-pem",
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mapTLSSettings(tt.tls)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestMapAuthenticationConfiguration(t *testing.T) {
	tests := []struct {
		name string
		auth AuthenticationConfiguration
		want *adminv2.AuthenticationConfiguration
	}{
		{
			name: "nil auth returns nil",
			auth: nil,
			want: nil,
		},
		{
			name: "SCRAM-SHA-256 configuration",
			auth: &ScramConfig{
				Username:       "alice",
				Password:       "secret",
				ScramMechanism: ScramMechanismScramSha256,
			},
			want: &adminv2.AuthenticationConfiguration{
				Authentication: &adminv2.AuthenticationConfiguration_ScramConfiguration{
					ScramConfiguration: &adminv2.ScramConfig{
						Username:       "alice",
						Password:       "secret",
						ScramMechanism: adminv2.ScramMechanism_SCRAM_MECHANISM_SCRAM_SHA_256,
					},
				},
			},
		},
		{
			name: "SCRAM-SHA-512 configuration",
			auth: &ScramConfig{
				Username:       "bob",
				Password:       "password",
				ScramMechanism: ScramMechanismScramSha512,
			},
			want: &adminv2.AuthenticationConfiguration{
				Authentication: &adminv2.AuthenticationConfiguration_ScramConfiguration{
					ScramConfiguration: &adminv2.ScramConfig{
						Username:       "bob",
						Password:       "password",
						ScramMechanism: adminv2.ScramMechanism_SCRAM_MECHANISM_SCRAM_SHA_512,
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mapAuthenticationConfiguration(tt.auth)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestMapScramMechanism(t *testing.T) {
	tests := []struct {
		name string
		mech ScramMechanism
		want adminv2.ScramMechanism
	}{
		{
			name: "SCRAM-SHA-256",
			mech: ScramMechanismScramSha256,
			want: adminv2.ScramMechanism_SCRAM_MECHANISM_SCRAM_SHA_256,
		},
		{
			name: "SCRAM-SHA-512",
			mech: ScramMechanismScramSha512,
			want: adminv2.ScramMechanism_SCRAM_MECHANISM_SCRAM_SHA_512,
		},
		{
			name: "unknown mechanism",
			mech: ScramMechanism("unknown"),
			want: adminv2.ScramMechanism_SCRAM_MECHANISM_UNSPECIFIED,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mapScramMechanism(tt.mech)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestMapTopicMetadataSyncOptions(t *testing.T) {
	tests := []struct {
		name string
		opts *TopicMetadataSyncOptions
		want *adminv2.TopicMetadataSyncOptions
	}{
		{
			name: "nil options returns nil",
			opts: nil,
			want: nil,
		},
		{
			name: "options with zero interval",
			opts: &TopicMetadataSyncOptions{
				Interval: 0,
			},
			want: &adminv2.TopicMetadataSyncOptions{},
		},
		{
			name: "complete options",
			opts: &TopicMetadataSyncOptions{
				Interval: 60 * time.Second,
				AutoCreateShadowTopicFilters: []*NameFilter{
					{PatternType: PatternTypeLiteral, FilterType: FilterTypeInclude, Name: "topic1"},
					{PatternType: PatternTypePrefix, FilterType: FilterTypeExclude, Name: "test-"},
				},
				SyncedShadowTopicProperties: []string{"retention.ms", "compression.type"},
				ExcludeDefault:              true,
			},
			want: &adminv2.TopicMetadataSyncOptions{
				Interval: durationpb.New(60 * time.Second),
				AutoCreateShadowTopicFilters: []*adminv2.NameFilter{
					{PatternType: adminv2.PatternType_PATTERN_TYPE_LITERAL, FilterType: adminv2.FilterType_FILTER_TYPE_INCLUDE, Name: "topic1"},
					{PatternType: adminv2.PatternType_PATTERN_TYPE_PREFIX, FilterType: adminv2.FilterType_FILTER_TYPE_EXCLUDE, Name: "test-"},
				},
				SyncedShadowTopicProperties: []string{"retention.ms", "compression.type"},
				ExcludeDefault:              true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mapTopicMetadataSyncOptions(tt.opts)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestMapConsumerOffsetSyncOptions(t *testing.T) {
	tests := []struct {
		name string
		opts *ConsumerOffsetSyncOptions
		want *adminv2.ConsumerOffsetSyncOptions
	}{
		{
			name: "nil options returns nil",
			opts: nil,
			want: nil,
		},
		{
			name: "disabled with zero interval",
			opts: &ConsumerOffsetSyncOptions{
				Enabled:  false,
				Interval: 0,
			},
			want: &adminv2.ConsumerOffsetSyncOptions{
				Enabled: false,
			},
		},
		{
			name: "enabled with filters",
			opts: &ConsumerOffsetSyncOptions{
				Enabled:  true,
				Interval: 45 * time.Second,
				GroupFilters: []*NameFilter{
					{PatternType: PatternTypeLiteral, FilterType: FilterTypeInclude, Name: "*"},
				},
			},
			want: &adminv2.ConsumerOffsetSyncOptions{
				Enabled:  true,
				Interval: durationpb.New(45 * time.Second),
				GroupFilters: []*adminv2.NameFilter{
					{PatternType: adminv2.PatternType_PATTERN_TYPE_LITERAL, FilterType: adminv2.FilterType_FILTER_TYPE_INCLUDE, Name: "*"},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mapConsumerOffsetSyncOptions(tt.opts)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestMapSecuritySyncOptions(t *testing.T) {
	tests := []struct {
		name string
		opts *SecuritySettingsSyncOptions
		want *adminv2.SecuritySettingsSyncOptions
	}{
		{
			name: "nil options returns nil",
			opts: nil,
			want: nil,
		},
		{
			name: "disabled with no filters",
			opts: &SecuritySettingsSyncOptions{
				Enabled:  false,
				Interval: 0,
			},
			want: &adminv2.SecuritySettingsSyncOptions{
				Enabled: false,
			},
		},
		{
			name: "enabled with ACL filters",
			opts: &SecuritySettingsSyncOptions{
				Enabled:  true,
				Interval: 120 * time.Second,
				ACLFilters: []*ACLFilter{
					{
						ResourceFilter: &ACLResourceFilter{
							ResourceType: ACLResourceTopic,
							PatternType:  ACLPatternLiteral,
							Name:         "sensitive-topic",
						},
						AccessFilter: &ACLAccessFilter{
							Principal:      "User:admin",
							Operation:      ACLOperationWrite,
							PermissionType: ACLPermissionTypeAllow,
							Host:           "192.168.1.1",
						},
					},
				},
			},
			want: &adminv2.SecuritySettingsSyncOptions{
				Enabled:  true,
				Interval: durationpb.New(120 * time.Second),
				AclFilters: []*adminv2.ACLFilter{
					{
						ResourceFilter: &adminv2.ACLResourceFilter{
							ResourceType: common.ACLResource_ACL_RESOURCE_TOPIC,
							PatternType:  common.ACLPattern_ACL_PATTERN_LITERAL,
							Name:         "sensitive-topic",
						},
						AccessFilter: &adminv2.ACLAccessFilter{
							Principal:      "User:admin",
							Operation:      common.ACLOperation_ACL_OPERATION_WRITE,
							PermissionType: common.ACLPermissionType_ACL_PERMISSION_TYPE_ALLOW,
							Host:           "192.168.1.1",
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mapSecuritySyncOptions(tt.opts)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestMapNameFilter(t *testing.T) {
	tests := []struct {
		name   string
		filter *NameFilter
		want   *adminv2.NameFilter
	}{
		{
			name:   "nil filter returns nil",
			filter: nil,
			want:   nil,
		},
		{
			name: "literal include filter",
			filter: &NameFilter{
				PatternType: PatternTypeLiteral,
				FilterType:  FilterTypeInclude,
				Name:        "my-topic",
			},
			want: &adminv2.NameFilter{
				PatternType: adminv2.PatternType_PATTERN_TYPE_LITERAL,
				FilterType:  adminv2.FilterType_FILTER_TYPE_INCLUDE,
				Name:        "my-topic",
			},
		},
		{
			name: "prefix exclude filter",
			filter: &NameFilter{
				PatternType: PatternTypePrefix,
				FilterType:  FilterTypeExclude,
				Name:        "internal-",
			},
			want: &adminv2.NameFilter{
				PatternType: adminv2.PatternType_PATTERN_TYPE_PREFIX,
				FilterType:  adminv2.FilterType_FILTER_TYPE_EXCLUDE,
				Name:        "internal-",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mapNameFilter(tt.filter)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestMapACLFilter(t *testing.T) {
	tests := []struct {
		name   string
		filter *ACLFilter
		want   *adminv2.ACLFilter
	}{
		{
			name:   "nil filter returns nil",
			filter: nil,
			want:   nil,
		},
		{
			name: "complete ACL filter",
			filter: &ACLFilter{
				ResourceFilter: &ACLResourceFilter{
					ResourceType: ACLResourceGroup,
					PatternType:  ACLPatternPrefixed,
					Name:         "consumer-",
				},
				AccessFilter: &ACLAccessFilter{
					Principal:      "User:consumer",
					Operation:      ACLOperationRead,
					PermissionType: ACLPermissionTypeAllow,
					Host:           "*",
				},
			},
			want: &adminv2.ACLFilter{
				ResourceFilter: &adminv2.ACLResourceFilter{
					ResourceType: common.ACLResource_ACL_RESOURCE_GROUP,
					PatternType:  common.ACLPattern_ACL_PATTERN_PREFIXED,
					Name:         "consumer-",
				},
				AccessFilter: &adminv2.ACLAccessFilter{
					Principal:      "User:consumer",
					Operation:      common.ACLOperation_ACL_OPERATION_READ,
					PermissionType: common.ACLPermissionType_ACL_PERMISSION_TYPE_ALLOW,
					Host:           "*",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mapACLFilter(tt.filter)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestMapPatternType(t *testing.T) {
	tests := []struct {
		name string
		pt   PatternType
		want adminv2.PatternType
	}{
		{
			name: "literal pattern",
			pt:   PatternTypeLiteral,
			want: adminv2.PatternType_PATTERN_TYPE_LITERAL,
		},
		{
			name: "prefix pattern",
			pt:   PatternTypePrefix,
			want: adminv2.PatternType_PATTERN_TYPE_PREFIX,
		},
		{
			name: "unknown pattern",
			pt:   PatternType("unknown"),
			want: adminv2.PatternType_PATTERN_TYPE_UNSPECIFIED,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mapPatternType(tt.pt)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestMapFilterType(t *testing.T) {
	tests := []struct {
		name string
		ft   FilterType
		want adminv2.FilterType
	}{
		{
			name: "include filter",
			ft:   FilterTypeInclude,
			want: adminv2.FilterType_FILTER_TYPE_INCLUDE,
		},
		{
			name: "exclude filter",
			ft:   FilterTypeExclude,
			want: adminv2.FilterType_FILTER_TYPE_EXCLUDE,
		},
		{
			name: "unknown filter",
			ft:   FilterType("unknown"),
			want: adminv2.FilterType_FILTER_TYPE_UNSPECIFIED,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mapFilterType(tt.ft)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestMapACLResource(t *testing.T) {
	tests := []struct {
		name     string
		resource ACLResource
		want     common.ACLResource
	}{
		{name: "any", resource: ACLResourceAny, want: common.ACLResource_ACL_RESOURCE_ANY},
		{name: "cluster", resource: ACLResourceCluster, want: common.ACLResource_ACL_RESOURCE_CLUSTER},
		{name: "group", resource: ACLResourceGroup, want: common.ACLResource_ACL_RESOURCE_GROUP},
		{name: "topic", resource: ACLResourceTopic, want: common.ACLResource_ACL_RESOURCE_TOPIC},
		{name: "txn_id", resource: ACLResourceTXNID, want: common.ACLResource_ACL_RESOURCE_TXN_ID},
		{name: "sr_subject", resource: ACLResourceSRSubject, want: common.ACLResource_ACL_RESOURCE_SR_SUBJECT},
		{name: "sr_registry", resource: ACLResourceSRRegistry, want: common.ACLResource_ACL_RESOURCE_SR_REGISTRY},
		{name: "sr_any", resource: ACLResourceSRAny, want: common.ACLResource_ACL_RESOURCE_SR_ANY},
		{name: "unknown", resource: ACLResource("unknown"), want: common.ACLResource_ACL_RESOURCE_UNSPECIFIED},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mapACLResource(tt.resource)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestMapACLPattern(t *testing.T) {
	tests := []struct {
		name    string
		pattern ACLPattern
		want    common.ACLPattern
	}{
		{name: "any", pattern: ACLPatternAny, want: common.ACLPattern_ACL_PATTERN_ANY},
		{name: "literal", pattern: ACLPatternLiteral, want: common.ACLPattern_ACL_PATTERN_LITERAL},
		{name: "prefixed", pattern: ACLPatternPrefixed, want: common.ACLPattern_ACL_PATTERN_PREFIXED},
		{name: "match", pattern: ACLPatternMatch, want: common.ACLPattern_ACL_PATTERN_MATCH},
		{name: "unknown", pattern: ACLPattern("unknown"), want: common.ACLPattern_ACL_PATTERN_UNSPECIFIED},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mapACLPattern(tt.pattern)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestMapACLOperation(t *testing.T) {
	tests := []struct {
		name      string
		operation ACLOperation
		want      common.ACLOperation
	}{
		{name: "any", operation: ACLOperationAny, want: common.ACLOperation_ACL_OPERATION_ANY},
		{name: "read", operation: ACLOperationRead, want: common.ACLOperation_ACL_OPERATION_READ},
		{name: "write", operation: ACLOperationWrite, want: common.ACLOperation_ACL_OPERATION_WRITE},
		{name: "create", operation: ACLOperationCreate, want: common.ACLOperation_ACL_OPERATION_CREATE},
		{name: "remove", operation: ACLOperationRemove, want: common.ACLOperation_ACL_OPERATION_REMOVE},
		{name: "alter", operation: ACLOperationAlter, want: common.ACLOperation_ACL_OPERATION_ALTER},
		{name: "describe", operation: ACLOperationDescribe, want: common.ACLOperation_ACL_OPERATION_DESCRIBE},
		{name: "cluster_action", operation: ACLOperationClusterAction, want: common.ACLOperation_ACL_OPERATION_CLUSTER_ACTION},
		{name: "describe_configs", operation: ACLOperationDescribeConfigs, want: common.ACLOperation_ACL_OPERATION_DESCRIBE_CONFIGS},
		{name: "alter_configs", operation: ACLOperationAlterConfigs, want: common.ACLOperation_ACL_OPERATION_ALTER_CONFIGS},
		{name: "idempotent_write", operation: ACLOperationIdempotentWrite, want: common.ACLOperation_ACL_OPERATION_IDEMPOTENT_WRITE},
		{name: "unknown", operation: ACLOperation("unknown"), want: common.ACLOperation_ACL_OPERATION_UNSPECIFIED},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mapACLOperation(tt.operation)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestMapACLPermissionType(t *testing.T) {
	tests := []struct {
		name     string
		permType ACLPermissionType
		want     common.ACLPermissionType
	}{
		{name: "any", permType: ACLPermissionTypeAny, want: common.ACLPermissionType_ACL_PERMISSION_TYPE_ANY},
		{name: "allow", permType: ACLPermissionTypeAllow, want: common.ACLPermissionType_ACL_PERMISSION_TYPE_ALLOW},
		{name: "deny", permType: ACLPermissionTypeDeny, want: common.ACLPermissionType_ACL_PERMISSION_TYPE_DENY},
		{name: "unknown", permType: ACLPermissionType("unknown"), want: common.ACLPermissionType_ACL_PERMISSION_TYPE_UNSPECIFIED},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mapACLPermissionType(tt.permType)
			require.Equal(t, tt.want, got)
		})
	}
}
