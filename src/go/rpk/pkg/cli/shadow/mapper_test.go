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

	controlplanev1 "buf.build/gen/go/redpandadata/cloud/protocolbuffers/go/redpanda/api/controlplane/v1"
	adminv2 "buf.build/gen/go/redpandadata/core/protocolbuffers/go/redpanda/core/admin/v2"
	corecommonv1 "buf.build/gen/go/redpandadata/core/protocolbuffers/go/redpanda/core/common/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestShadowLinkConfigToAdmin(t *testing.T) {
	tests := []struct {
		name string
		cfg  *ShadowLinkConfig
		want *adminv2.ShadowLink
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
			want: &adminv2.ShadowLink{
				Name:           "test-link",
				Configurations: &adminv2.ShadowLinkConfigurations{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := shadowLinkConfigToProto(tt.cfg)
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
				BootstrapServers:       []string{"localhost:9092"},
				SourceClusterID:        "test-cluster",
				MetadataMaxAgeMs:       5000,
				ConnectionTimeoutMs:    2000,
				RetryBackoffMs:         200,
				FetchWaitMaxMs:         1000,
				FetchMinBytes:          10,
				FetchMaxBytes:          2097152,
				FetchPartitionMaxBytes: 1048576,
			},
			want: &adminv2.ShadowLinkClientOptions{
				BootstrapServers:       []string{"localhost:9092"},
				SourceClusterId:        "test-cluster",
				MetadataMaxAgeMs:       5000,
				ConnectionTimeoutMs:    2000,
				RetryBackoffMs:         200,
				FetchWaitMaxMs:         1000,
				FetchMinBytes:          10,
				FetchMaxBytes:          2097152,
				FetchPartitionMaxBytes: 1048576,
			},
		},
		{
			name: "with file-based TLS",
			opts: &ShadowLinkClientOptions{
				BootstrapServers: []string{"localhost:9092"},
				TLSSettings: &TLSSettings{
					Enabled:             true,
					DoNotSetSniHostname: true,
					TLSFileSettings: &TLSFileSettings{
						CAPath:   "/ca.crt",
						KeyPath:  "/key.pem",
						CertPath: "/cert.pem",
					},
				},
			},
			want: &adminv2.ShadowLinkClientOptions{
				BootstrapServers: []string{"localhost:9092"},
				TlsSettings: &corecommonv1.TLSSettings{
					Enabled:             true,
					DoNotSetSniHostname: true,
					TlsSettings: &corecommonv1.TLSSettings_TlsFileSettings{
						TlsFileSettings: &corecommonv1.TLSFileSettings{
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
				TLSSettings: &TLSSettings{
					Enabled:             true,
					DoNotSetSniHostname: true,
					TLSPEMSettings: &TLSPEMSettings{
						CA:   "ca-content",
						Key:  "key-content",
						Cert: "cert-content",
					},
				},
			},
			want: &adminv2.ShadowLinkClientOptions{
				BootstrapServers: []string{"localhost:9092"},
				TlsSettings: &corecommonv1.TLSSettings{
					Enabled:             true,
					DoNotSetSniHostname: true,
					TlsSettings: &corecommonv1.TLSSettings_TlsPemSettings{
						TlsPemSettings: &corecommonv1.TLSPEMSettings{
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
				AuthenticationConfiguration: &AuthenticationConfiguration{
					ScramConfiguration: &ScramConfiguration{
						Username:       "user",
						Password:       "pass",
						ScramMechanism: ScramMechanismScramSha512,
					},
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
		tls  *TLSSettings
		want *corecommonv1.TLSSettings
	}{
		{
			name: "nil TLS returns nil",
			tls:  nil,
			want: nil,
		},
		{
			name: "file-based TLS settings",
			tls: &TLSSettings{
				Enabled: true,
				TLSFileSettings: &TLSFileSettings{
					CAPath:   "/path/to/ca",
					KeyPath:  "/path/to/key",
					CertPath: "/path/to/cert",
				},
			},
			want: &corecommonv1.TLSSettings{
				Enabled: true,
				TlsSettings: &corecommonv1.TLSSettings_TlsFileSettings{
					TlsFileSettings: &corecommonv1.TLSFileSettings{
						CaPath:   "/path/to/ca",
						KeyPath:  "/path/to/key",
						CertPath: "/path/to/cert",
					},
				},
			},
		},
		{
			name: "PEM-based TLS settings",
			tls: &TLSSettings{
				Enabled: false,
				TLSPEMSettings: &TLSPEMSettings{
					CA:   "ca-pem",
					Key:  "key-pem",
					Cert: "cert-pem",
				},
			},
			want: &corecommonv1.TLSSettings{
				Enabled: false,
				TlsSettings: &corecommonv1.TLSSettings_TlsPemSettings{
					TlsPemSettings: &corecommonv1.TLSPEMSettings{
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
		auth *AuthenticationConfiguration
		want *adminv2.AuthenticationConfiguration
	}{
		{
			name: "nil auth returns nil",
			auth: nil,
			want: nil,
		},
		{
			name: "SCRAM-SHA-256 configuration",
			auth: &AuthenticationConfiguration{
				ScramConfiguration: &ScramConfiguration{
					Username:       "alice",
					Password:       "secret",
					ScramMechanism: ScramMechanismScramSha256,
				},
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
			auth: &AuthenticationConfiguration{
				ScramConfiguration: &ScramConfiguration{
					Username:       "bob",
					Password:       "password",
					ScramMechanism: ScramMechanismScramSha512,
				},
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
			name: "with filters and properties",
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
			name: "paused with zero interval",
			opts: &ConsumerOffsetSyncOptions{
				Paused:   true,
				Interval: 0,
			},
			want: &adminv2.ConsumerOffsetSyncOptions{
				Paused: true,
			},
		},
		{
			name: "not paused with filters",
			opts: &ConsumerOffsetSyncOptions{
				Paused:   false,
				Interval: 45 * time.Second,
				GroupFilters: []*NameFilter{
					{PatternType: PatternTypeLiteral, FilterType: FilterTypeInclude, Name: "*"},
				},
			},
			want: &adminv2.ConsumerOffsetSyncOptions{
				Paused:   false,
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
			name: "paused with no filters",
			opts: &SecuritySettingsSyncOptions{
				Paused:   true,
				Interval: 0,
			},
			want: &adminv2.SecuritySettingsSyncOptions{
				Paused: true,
			},
		},
		{
			name: "not paused with ACL filters",
			opts: &SecuritySettingsSyncOptions{
				Paused:   false,
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
				Paused:   false,
				Interval: durationpb.New(120 * time.Second),
				AclFilters: []*adminv2.ACLFilter{
					{
						ResourceFilter: &adminv2.ACLResourceFilter{
							ResourceType: corecommonv1.ACLResource_ACL_RESOURCE_TOPIC,
							PatternType:  corecommonv1.ACLPattern_ACL_PATTERN_LITERAL,
							Name:         "sensitive-topic",
						},
						AccessFilter: &adminv2.ACLAccessFilter{
							Principal:      "User:admin",
							Operation:      corecommonv1.ACLOperation_ACL_OPERATION_WRITE,
							PermissionType: corecommonv1.ACLPermissionType_ACL_PERMISSION_TYPE_ALLOW,
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
					ResourceType: corecommonv1.ACLResource_ACL_RESOURCE_GROUP,
					PatternType:  corecommonv1.ACLPattern_ACL_PATTERN_PREFIXED,
					Name:         "consumer-",
				},
				AccessFilter: &adminv2.ACLAccessFilter{
					Principal:      "User:consumer",
					Operation:      corecommonv1.ACLOperation_ACL_OPERATION_READ,
					PermissionType: corecommonv1.ACLPermissionType_ACL_PERMISSION_TYPE_ALLOW,
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

// Reverse mapping tests (admin proto -> config)

func TestShadowLinkToConfig(t *testing.T) {
	tests := []struct {
		name string
		sl   *adminv2.ShadowLink
		want *ShadowLinkConfig
	}{
		{
			name: "nil shadow link returns nil",
			sl:   nil,
			want: nil,
		},
		{
			name: "minimal shadow link",
			sl: &adminv2.ShadowLink{
				Name:           "test-link",
				Configurations: nil,
			},
			want: &ShadowLinkConfig{
				Name: "test-link",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := shadowLinkToConfig(tt.sl)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestAdminClientOptsToCfg(t *testing.T) {
	tests := []struct {
		name string
		opts *adminv2.ShadowLinkClientOptions
		want *ShadowLinkClientOptions
	}{
		{
			name: "nil options returns nil",
			opts: nil,
			want: nil,
		},
		{
			name: "basic options without TLS or auth",
			opts: &adminv2.ShadowLinkClientOptions{
				BootstrapServers:       []string{"localhost:9092"},
				SourceClusterId:        "test-cluster",
				MetadataMaxAgeMs:       5000,
				ConnectionTimeoutMs:    2000,
				RetryBackoffMs:         200,
				FetchWaitMaxMs:         1000,
				FetchMinBytes:          10,
				FetchMaxBytes:          2097152,
				FetchPartitionMaxBytes: 10,
			},
			want: &ShadowLinkClientOptions{
				BootstrapServers:       []string{"localhost:9092"},
				SourceClusterID:        "test-cluster",
				MetadataMaxAgeMs:       5000,
				ConnectionTimeoutMs:    2000,
				RetryBackoffMs:         200,
				FetchWaitMaxMs:         1000,
				FetchMinBytes:          10,
				FetchMaxBytes:          2097152,
				FetchPartitionMaxBytes: 10,
			},
		},
		{
			name: "with file-based TLS",
			opts: &adminv2.ShadowLinkClientOptions{
				BootstrapServers: []string{"localhost:9092"},
				TlsSettings: &corecommonv1.TLSSettings{
					Enabled: true,
					TlsSettings: &corecommonv1.TLSSettings_TlsFileSettings{
						TlsFileSettings: &corecommonv1.TLSFileSettings{
							CaPath:   "/ca.crt",
							KeyPath:  "/key.pem",
							CertPath: "/cert.pem",
						},
					},
				},
			},
			want: &ShadowLinkClientOptions{
				BootstrapServers: []string{"localhost:9092"},
				TLSSettings: &TLSSettings{
					Enabled: true,
					TLSFileSettings: &TLSFileSettings{
						CAPath:   "/ca.crt",
						KeyPath:  "/key.pem",
						CertPath: "/cert.pem",
					},
				},
			},
		},
		{
			name: "with PEM-based TLS",
			opts: &adminv2.ShadowLinkClientOptions{
				BootstrapServers: []string{"localhost:9092"},
				TlsSettings: &corecommonv1.TLSSettings{
					Enabled: true,
					TlsSettings: &corecommonv1.TLSSettings_TlsPemSettings{
						TlsPemSettings: &corecommonv1.TLSPEMSettings{
							Ca:   "ca-content",
							Key:  "key-content",
							Cert: "cert-content",
						},
					},
				},
			},
			want: &ShadowLinkClientOptions{
				BootstrapServers: []string{"localhost:9092"},
				TLSSettings: &TLSSettings{
					Enabled: true,
					TLSPEMSettings: &TLSPEMSettings{
						CA:   "ca-content",
						Key:  "key-content",
						Cert: "cert-content",
					},
				},
			},
		},
		{
			name: "with SCRAM authentication",
			opts: &adminv2.ShadowLinkClientOptions{
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
			want: &ShadowLinkClientOptions{
				BootstrapServers: []string{"localhost:9092"},
				AuthenticationConfiguration: &AuthenticationConfiguration{
					ScramConfiguration: &ScramConfiguration{
						Username:       "user",
						Password:       "pass",
						ScramMechanism: ScramMechanismScramSha512,
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := adminClientOptsToCfg(tt.opts)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestAdminTLSToCfg(t *testing.T) {
	tests := []struct {
		name string
		tls  *corecommonv1.TLSSettings
		want *TLSSettings
	}{
		{
			name: "nil TLS returns nil",
			tls:  nil,
			want: nil,
		},
		{
			name: "file-based TLS settings",
			tls: &corecommonv1.TLSSettings{
				Enabled: true,
				TlsSettings: &corecommonv1.TLSSettings_TlsFileSettings{
					TlsFileSettings: &corecommonv1.TLSFileSettings{
						CaPath:   "/path/to/ca",
						KeyPath:  "/path/to/key",
						CertPath: "/path/to/cert",
					},
				},
			},
			want: &TLSSettings{
				Enabled: true,
				TLSFileSettings: &TLSFileSettings{
					CAPath:   "/path/to/ca",
					KeyPath:  "/path/to/key",
					CertPath: "/path/to/cert",
				},
			},
		},
		{
			name: "PEM-based TLS settings",
			tls: &corecommonv1.TLSSettings{
				Enabled: false,
				TlsSettings: &corecommonv1.TLSSettings_TlsPemSettings{
					TlsPemSettings: &corecommonv1.TLSPEMSettings{
						Ca:   "ca-pem",
						Key:  "key-pem",
						Cert: "cert-pem",
					},
				},
			},
			want: &TLSSettings{
				Enabled: false,
				TLSPEMSettings: &TLSPEMSettings{
					CA:   "ca-pem",
					Key:  "key-pem",
					Cert: "cert-pem",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := adminTLSToCfg(tt.tls)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestAdminAuthToCfg(t *testing.T) {
	tests := []struct {
		name string
		auth *adminv2.AuthenticationConfiguration
		want *AuthenticationConfiguration
	}{
		{
			name: "nil auth returns nil",
			auth: nil,
			want: nil,
		},
		{
			name: "SCRAM-SHA-256 configuration",
			auth: &adminv2.AuthenticationConfiguration{
				Authentication: &adminv2.AuthenticationConfiguration_ScramConfiguration{
					ScramConfiguration: &adminv2.ScramConfig{
						Username:       "alice",
						Password:       "secret",
						ScramMechanism: adminv2.ScramMechanism_SCRAM_MECHANISM_SCRAM_SHA_256,
					},
				},
			},
			want: &AuthenticationConfiguration{
				ScramConfiguration: &ScramConfiguration{
					Username:       "alice",
					Password:       "secret",
					ScramMechanism: ScramMechanismScramSha256,
				},
			},
		},
		{
			name: "SCRAM-SHA-512 configuration",
			auth: &adminv2.AuthenticationConfiguration{
				Authentication: &adminv2.AuthenticationConfiguration_ScramConfiguration{
					ScramConfiguration: &adminv2.ScramConfig{
						Username:       "bob",
						Password:       "password",
						ScramMechanism: adminv2.ScramMechanism_SCRAM_MECHANISM_SCRAM_SHA_512,
					},
				},
			},
			want: &AuthenticationConfiguration{
				ScramConfiguration: &ScramConfiguration{
					Username:       "bob",
					Password:       "password",
					ScramMechanism: ScramMechanismScramSha512,
				},
			},
		},
		{
			name: "nil SCRAM configuration returns nil",
			auth: &adminv2.AuthenticationConfiguration{
				Authentication: &adminv2.AuthenticationConfiguration_ScramConfiguration{
					ScramConfiguration: nil,
				},
			},
			want: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := adminAuthToCfg(tt.auth)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestAdminTopicMetadataSyncToCfg(t *testing.T) {
	tests := []struct {
		name string
		opts *adminv2.TopicMetadataSyncOptions
		want *TopicMetadataSyncOptions
	}{
		{
			name: "nil options returns nil",
			opts: nil,
			want: nil,
		},
		{
			name: "options with nil interval",
			opts: &adminv2.TopicMetadataSyncOptions{
				Interval: nil,
			},
			want: &TopicMetadataSyncOptions{
				Interval: 0,
			},
		},
		{
			name: "with filters and properties",
			opts: &adminv2.TopicMetadataSyncOptions{
				Interval: durationpb.New(60 * time.Second),
				AutoCreateShadowTopicFilters: []*adminv2.NameFilter{
					{PatternType: adminv2.PatternType_PATTERN_TYPE_LITERAL, FilterType: adminv2.FilterType_FILTER_TYPE_INCLUDE, Name: "topic1"},
					{PatternType: adminv2.PatternType_PATTERN_TYPE_PREFIX, FilterType: adminv2.FilterType_FILTER_TYPE_EXCLUDE, Name: "test-"},
				},
				SyncedShadowTopicProperties: []string{"retention.ms", "compression.type"},
				ExcludeDefault:              true,
			},
			want: &TopicMetadataSyncOptions{
				Interval: 60 * time.Second,
				AutoCreateShadowTopicFilters: []*NameFilter{
					{PatternType: PatternTypeLiteral, FilterType: FilterTypeInclude, Name: "topic1"},
					{PatternType: PatternTypePrefix, FilterType: FilterTypeExclude, Name: "test-"},
				},
				SyncedShadowTopicProperties: []string{"retention.ms", "compression.type"},
				ExcludeDefault:              true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := adminTopicMetadataSyncToCfg(tt.opts)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestAdminConsumerOffsetSyncToCfg(t *testing.T) {
	tests := []struct {
		name string
		opts *adminv2.ConsumerOffsetSyncOptions
		want *ConsumerOffsetSyncOptions
	}{
		{
			name: "nil options returns nil",
			opts: nil,
			want: nil,
		},
		{
			name: "paused with nil interval",
			opts: &adminv2.ConsumerOffsetSyncOptions{
				Paused:   true,
				Interval: nil,
			},
			want: &ConsumerOffsetSyncOptions{
				Paused:   true,
				Interval: 0,
			},
		},
		{
			name: "not paused with filters",
			opts: &adminv2.ConsumerOffsetSyncOptions{
				Paused:   false,
				Interval: durationpb.New(45 * time.Second),
				GroupFilters: []*adminv2.NameFilter{
					{PatternType: adminv2.PatternType_PATTERN_TYPE_LITERAL, FilterType: adminv2.FilterType_FILTER_TYPE_INCLUDE, Name: "*"},
				},
			},
			want: &ConsumerOffsetSyncOptions{
				Paused:   false,
				Interval: 45 * time.Second,
				GroupFilters: []*NameFilter{
					{PatternType: PatternTypeLiteral, FilterType: FilterTypeInclude, Name: "*"},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := adminConsumerOffsetSyncToCfg(tt.opts)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestAdminSecuritySyncToCfg(t *testing.T) {
	tests := []struct {
		name string
		opts *adminv2.SecuritySettingsSyncOptions
		want *SecuritySettingsSyncOptions
	}{
		{
			name: "nil options returns nil",
			opts: nil,
			want: nil,
		},
		{
			name: "paused with no filters",
			opts: &adminv2.SecuritySettingsSyncOptions{
				Paused:   true,
				Interval: nil,
			},
			want: &SecuritySettingsSyncOptions{
				Paused:   true,
				Interval: 0,
			},
		},
		{
			name: "not paused with ACL filters",
			opts: &adminv2.SecuritySettingsSyncOptions{
				Paused:   false,
				Interval: durationpb.New(120 * time.Second),
				AclFilters: []*adminv2.ACLFilter{
					{
						ResourceFilter: &adminv2.ACLResourceFilter{
							ResourceType: corecommonv1.ACLResource_ACL_RESOURCE_TOPIC,
							PatternType:  corecommonv1.ACLPattern_ACL_PATTERN_LITERAL,
							Name:         "sensitive-topic",
						},
						AccessFilter: &adminv2.ACLAccessFilter{
							Principal:      "User:admin",
							Operation:      corecommonv1.ACLOperation_ACL_OPERATION_WRITE,
							PermissionType: corecommonv1.ACLPermissionType_ACL_PERMISSION_TYPE_ALLOW,
							Host:           "192.168.1.1",
						},
					},
				},
			},
			want: &SecuritySettingsSyncOptions{
				Paused:   false,
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
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := adminSecuritySyncToCfg(tt.opts)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestAdminMapFilterToCfg(t *testing.T) {
	tests := []struct {
		name   string
		filter *adminv2.NameFilter
		want   *NameFilter
	}{
		{
			name:   "nil filter returns nil",
			filter: nil,
			want:   nil,
		},
		{
			name: "literal include filter",
			filter: &adminv2.NameFilter{
				PatternType: adminv2.PatternType_PATTERN_TYPE_LITERAL,
				FilterType:  adminv2.FilterType_FILTER_TYPE_INCLUDE,
				Name:        "my-topic",
			},
			want: &NameFilter{
				PatternType: PatternTypeLiteral,
				FilterType:  FilterTypeInclude,
				Name:        "my-topic",
			},
		},
		{
			name: "prefix exclude filter",
			filter: &adminv2.NameFilter{
				PatternType: adminv2.PatternType_PATTERN_TYPE_PREFIX,
				FilterType:  adminv2.FilterType_FILTER_TYPE_EXCLUDE,
				Name:        "internal-",
			},
			want: &NameFilter{
				PatternType: PatternTypePrefix,
				FilterType:  FilterTypeExclude,
				Name:        "internal-",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := adminMapFilterToCfg(tt.filter)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestAdminACLFilterToCfg(t *testing.T) {
	tests := []struct {
		name   string
		filter *adminv2.ACLFilter
		want   *ACLFilter
	}{
		{
			name:   "nil filter returns nil",
			filter: nil,
			want:   nil,
		},
		{
			name: "complete ACL filter",
			filter: &adminv2.ACLFilter{
				ResourceFilter: &adminv2.ACLResourceFilter{
					ResourceType: corecommonv1.ACLResource_ACL_RESOURCE_GROUP,
					PatternType:  corecommonv1.ACLPattern_ACL_PATTERN_PREFIXED,
					Name:         "consumer-",
				},
				AccessFilter: &adminv2.ACLAccessFilter{
					Principal:      "User:consumer",
					Operation:      corecommonv1.ACLOperation_ACL_OPERATION_READ,
					PermissionType: corecommonv1.ACLPermissionType_ACL_PERMISSION_TYPE_ALLOW,
					Host:           "*",
				},
			},
			want: &ACLFilter{
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
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := adminACLFilterToCfg(tt.filter)
			require.Equal(t, tt.want, got)
		})
	}
}

// Round-trip test: config -> proto -> config
// This comprehensive test validates the entire bidirectional mapping chain works correctly
func TestRoundTrip(t *testing.T) {
	// Create a fully populated config with all possible options
	originalConfig := &ShadowLinkConfig{
		Name: "complete-round-trip-link",
		ClientOptions: &ShadowLinkClientOptions{
			BootstrapServers:       []string{"broker1:9092", "broker2:9092", "broker3:9092"},
			SourceClusterID:        "prod-cluster-xyz",
			MetadataMaxAgeMs:       15000,
			ConnectionTimeoutMs:    3000,
			RetryBackoffMs:         250,
			FetchWaitMaxMs:         750,
			FetchMinBytes:          2048,
			FetchMaxBytes:          20971520,
			FetchPartitionMaxBytes: 512,
			TLSSettings: &TLSSettings{
				Enabled:             true,
				DoNotSetSniHostname: true,
				TLSFileSettings: &TLSFileSettings{
					CAPath:   "/etc/ssl/certs/ca-bundle.pem",
					KeyPath:  "/etc/ssl/private/client-key.pem",
					CertPath: "/etc/ssl/certs/client-cert.pem",
				},
			},
			AuthenticationConfiguration: &AuthenticationConfiguration{
				ScramConfiguration: &ScramConfiguration{
					Username:       "shadow-replication-user",
					Password:       "super-secure-password-123",
					ScramMechanism: ScramMechanismScramSha512,
				},
			},
		},
		SchemaRegistrySyncOptions: &SchemaRegistrySyncOptions{
			ShadowSchemaRegistryTopic: &ShadowSchemaRegistryTopic{},
		},
		TopicMetadataSyncOptions: &TopicMetadataSyncOptions{
			Interval: 45 * time.Second,
			Paused:   true,
			AutoCreateShadowTopicFilters: []*NameFilter{
				{
					PatternType: PatternTypeLiteral,
					FilterType:  FilterTypeInclude,
					Name:        "orders",
				},
				{
					PatternType: PatternTypeLiteral,
					FilterType:  FilterTypeInclude,
					Name:        "payments",
				},
				{
					PatternType: PatternTypePrefix,
					FilterType:  FilterTypeExclude,
					Name:        "internal-",
				},
				{
					PatternType: PatternTypePrefix,
					FilterType:  FilterTypeExclude,
					Name:        "_",
				},
			},
			SyncedShadowTopicProperties: []string{
				"retention.ms",
				"retention.bytes",
				"compression.type",
				"cleanup.policy",
				"min.compaction.lag.ms",
			},
			ExcludeDefault:  false,
			StartAtEarliest: &StartAtEarliest{},
		},
		ConsumerOffsetSyncOptions: &ConsumerOffsetSyncOptions{
			Paused:   true,
			Interval: 90 * time.Second,
			GroupFilters: []*NameFilter{
				{
					PatternType: PatternTypePrefix,
					FilterType:  FilterTypeInclude,
					Name:        "prod-",
				},
				{
					PatternType: PatternTypeLiteral,
					FilterType:  FilterTypeExclude,
					Name:        "test-group",
				},
			},
		},
		SecuritySyncOptions: &SecuritySettingsSyncOptions{
			Paused:   true,
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
						Operation:      ACLOperationRead,
						PermissionType: ACLPermissionTypeAllow,
						Host:           "*",
					},
				},
				{
					ResourceFilter: &ACLResourceFilter{
						ResourceType: ACLResourceGroup,
						PatternType:  ACLPatternPrefixed,
						Name:         "consumer-",
					},
					AccessFilter: &ACLAccessFilter{
						Principal:      "User:service-account",
						Operation:      ACLOperationWrite,
						PermissionType: ACLPermissionTypeDeny,
						Host:           "192.168.1.100",
					},
				},
				{
					ResourceFilter: &ACLResourceFilter{
						ResourceType: ACLResourceCluster,
						PatternType:  ACLPatternLiteral,
						Name:         "kafka-cluster",
					},
					AccessFilter: &ACLAccessFilter{
						Principal:      "User:cluster-admin",
						Operation:      ACLOperationClusterAction,
						PermissionType: ACLPermissionTypeAllow,
						Host:           "*",
					},
				},
			},
		},
	}

	// Step 1: Convert config to proto (config -> proto)
	adminShadowLink := shadowLinkConfigToProto(originalConfig)
	require.NotNil(t, adminShadowLink, "shadow link should not be nil")

	// Step 2: Convert proto back to config (proto -> config)
	roundTripConfig := shadowLinkToConfig(adminShadowLink)
	require.NotNil(t, roundTripConfig, "round-trip config should not be nil")

	// Step 3: Verify the round-trip config matches the original
	require.Equal(t, originalConfig, roundTripConfig, "round-trip config should exactly match original config")
}

// Cloud mapping tests

func TestCloudShadowLinkToConfig(t *testing.T) {
	tests := []struct {
		name string
		sl   *controlplanev1.ShadowLink
		want *ShadowLinkConfig
	}{
		{
			name: "nil shadow link returns nil",
			sl:   nil,
			want: nil,
		},
		{
			name: "minimal shadow link",
			sl: &controlplanev1.ShadowLink{
				Name:             "test-link",
				ShadowRedpandaId: "rp-456",
			},
			want: &ShadowLinkConfig{
				Name: "test-link",
				CloudOptions: &CloudShadowLinkOptions{
					ShadowRedpandaID: "rp-456",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := cloudShadowLinkToConfig(tt.sl)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestCloudClientOptsToCfg(t *testing.T) {
	tests := []struct {
		name string
		opts *controlplanev1.ShadowLinkClientOptions
		want *ShadowLinkClientOptions
	}{
		{
			name: "nil options returns nil",
			opts: nil,
			want: nil,
		},
		{
			name: "basic options without TLS or auth",
			opts: &controlplanev1.ShadowLinkClientOptions{
				BootstrapServers:       []string{"cloud-broker:9092"},
				SourceClusterId:        "source-cluster",
				MetadataMaxAgeMs:       5000,
				ConnectionTimeoutMs:    2000,
				RetryBackoffMs:         200,
				FetchWaitMaxMs:         1000,
				FetchMinBytes:          10,
				FetchMaxBytes:          2097152,
				FetchPartitionMaxBytes: 1048576,
			},
			want: &ShadowLinkClientOptions{
				BootstrapServers:       []string{"cloud-broker:9092"},
				SourceClusterID:        "source-cluster",
				MetadataMaxAgeMs:       5000,
				ConnectionTimeoutMs:    2000,
				RetryBackoffMs:         200,
				FetchWaitMaxMs:         1000,
				FetchMinBytes:          10,
				FetchMaxBytes:          2097152,
				FetchPartitionMaxBytes: 1048576,
			},
		},
		{
			name: "with PEM-based TLS",
			opts: &controlplanev1.ShadowLinkClientOptions{
				BootstrapServers: []string{"cloud-broker:9092"},
				TlsSettings: &controlplanev1.TLSSettings{
					Enabled:             true,
					DoNotSetSniHostname: true,
					Ca:                  "ca-pem-content",
					Key:                 "key-pem-content",
					Cert:                "cert-pem-content",
				},
			},
			want: &ShadowLinkClientOptions{
				BootstrapServers: []string{"cloud-broker:9092"},
				TLSSettings: &TLSSettings{
					Enabled:             true,
					DoNotSetSniHostname: true,
					TLSPEMSettings: &TLSPEMSettings{
						CA:   "ca-pem-content",
						Key:  "key-pem-content",
						Cert: "cert-pem-content",
					},
				},
			},
		},
		{
			name: "with SCRAM authentication",
			opts: &controlplanev1.ShadowLinkClientOptions{
				BootstrapServers: []string{"cloud-broker:9092"},
				AuthenticationConfiguration: &adminv2.AuthenticationConfiguration{
					Authentication: &adminv2.AuthenticationConfiguration_ScramConfiguration{
						ScramConfiguration: &adminv2.ScramConfig{
							Username:       "cloud-user",
							Password:       "${secrets.MY_PASSWORD}",
							ScramMechanism: adminv2.ScramMechanism_SCRAM_MECHANISM_SCRAM_SHA_256,
						},
					},
				},
			},
			want: &ShadowLinkClientOptions{
				BootstrapServers: []string{"cloud-broker:9092"},
				AuthenticationConfiguration: &AuthenticationConfiguration{
					ScramConfiguration: &ScramConfiguration{
						Username:       "cloud-user",
						Password:       "${secrets.MY_PASSWORD}",
						ScramMechanism: ScramMechanismScramSha256,
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := cloudClientOptsToCfg(tt.opts)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestCloudTLSToCfg(t *testing.T) {
	tests := []struct {
		name string
		tls  *controlplanev1.TLSSettings
		want *TLSSettings
	}{
		{
			name: "nil TLS returns nil",
			tls:  nil,
			want: nil,
		},
		{
			name: "enabled TLS with PEM content",
			tls: &controlplanev1.TLSSettings{
				Enabled:             true,
				DoNotSetSniHostname: false,
				Ca:                  "-----BEGIN CERTIFICATE-----\nCA\n-----END CERTIFICATE-----",
				Key:                 "-----BEGIN PRIVATE KEY-----\nKEY\n-----END PRIVATE KEY-----",
				Cert:                "-----BEGIN CERTIFICATE-----\nCERT\n-----END CERTIFICATE-----",
			},
			want: &TLSSettings{
				Enabled:             true,
				DoNotSetSniHostname: false,
				TLSPEMSettings: &TLSPEMSettings{
					CA:   "-----BEGIN CERTIFICATE-----\nCA\n-----END CERTIFICATE-----",
					Key:  "-----BEGIN PRIVATE KEY-----\nKEY\n-----END PRIVATE KEY-----",
					Cert: "-----BEGIN CERTIFICATE-----\nCERT\n-----END CERTIFICATE-----",
				},
			},
		},
		{
			name: "TLS enabled without PEM content",
			tls: &controlplanev1.TLSSettings{
				Enabled: true,
			},
			want: &TLSSettings{
				Enabled: true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := cloudTLSToCfg(tt.tls)
			require.Equal(t, tt.want, got)
		})
	}
}

// TestCloudRoundTrip validates the complete cloud mapping chain:
// ShadowLinkConfig -> Cloud protos -> back to ShadowLinkConfig.
// This test ensures cloud-specific fields are properly preserved.
func TestCloudRoundTrip(t *testing.T) {
	originalConfig := &ShadowLinkConfig{
		Name: "cloud-round-trip-link",
		CloudOptions: &CloudShadowLinkOptions{
			ShadowRedpandaID: "shadow-rp-xyz",
			SourceRedpandaID: "source-rp-123",
		},
		ClientOptions: &ShadowLinkClientOptions{
			BootstrapServers:       []string{"broker1.cloud:9092", "broker2.cloud:9092"},
			SourceClusterID:        "prod-source-cluster",
			MetadataMaxAgeMs:       15000,
			ConnectionTimeoutMs:    3000,
			RetryBackoffMs:         250,
			FetchWaitMaxMs:         750,
			FetchMinBytes:          2048,
			FetchMaxBytes:          20971520,
			FetchPartitionMaxBytes: 1048576,
			TLSSettings: &TLSSettings{
				Enabled:             true,
				DoNotSetSniHostname: true,
				TLSPEMSettings: &TLSPEMSettings{
					CA:   "-----BEGIN CERTIFICATE-----\nCA-CONTENT\n-----END CERTIFICATE-----",
					Key:  "-----BEGIN PRIVATE KEY-----\nKEY-CONTENT\n-----END PRIVATE KEY-----",
					Cert: "-----BEGIN CERTIFICATE-----\nCERT-CONTENT\n-----END CERTIFICATE-----",
				},
			},
			AuthenticationConfiguration: &AuthenticationConfiguration{
				ScramConfiguration: &ScramConfiguration{
					Username:       "cloud-replication-user",
					Password:       "${secrets.CLOUD_PASSWORD}",
					ScramMechanism: ScramMechanismScramSha512,
				},
			},
		},
		TopicMetadataSyncOptions: &TopicMetadataSyncOptions{
			Interval: 45 * time.Second,
			Paused:   false,
			AutoCreateShadowTopicFilters: []*NameFilter{
				{
					PatternType: PatternTypeLiteral,
					FilterType:  FilterTypeInclude,
					Name:        "orders",
				},
				{
					PatternType: PatternTypePrefix,
					FilterType:  FilterTypeExclude,
					Name:        "_internal-",
				},
			},
			SyncedShadowTopicProperties: []string{
				"retention.ms",
				"compression.type",
			},
			ExcludeDefault:  true,
			StartAtEarliest: &StartAtEarliest{},
		},
		ConsumerOffsetSyncOptions: &ConsumerOffsetSyncOptions{
			Paused:   false,
			Interval: 60 * time.Second,
			GroupFilters: []*NameFilter{
				{
					PatternType: PatternTypePrefix,
					FilterType:  FilterTypeInclude,
					Name:        "prod-",
				},
			},
		},
		SecuritySyncOptions: &SecuritySettingsSyncOptions{
			Paused:   true,
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
						Operation:      ACLOperationRead,
						PermissionType: ACLPermissionTypeAllow,
						Host:           "*",
					},
				},
			},
		},
		SchemaRegistrySyncOptions: &SchemaRegistrySyncOptions{
			ShadowSchemaRegistryTopic: &ShadowSchemaRegistryTopic{},
		},
	}

	// Step 1: Convert config to cloud ShadowLink proto (simulating what the API returns)
	// We build this manually since shadowLinkConfigToCloudCreate creates ShadowLinkCreate,
	// and the API returns ShadowLink (which has additional fields like Id).
	cloudSL := &controlplanev1.ShadowLink{
		Id:                        "sl-generated-id",
		ShadowRedpandaId:          originalConfig.CloudOptions.ShadowRedpandaID,
		Name:                      originalConfig.Name,
		ClientOptions:             mapCloudClientOptions(originalConfig.ClientOptions),
		TopicMetadataSyncOptions:  mapTopicMetadataSyncOptions(originalConfig.TopicMetadataSyncOptions),
		ConsumerOffsetSyncOptions: mapConsumerOffsetSyncOptions(originalConfig.ConsumerOffsetSyncOptions),
		SecuritySyncOptions:       mapSecuritySyncOptions(originalConfig.SecuritySyncOptions),
		SchemaRegistrySyncOptions: mapSchemaRegistrySyncOptions(originalConfig.SchemaRegistrySyncOptions),
	}
	require.NotNil(t, cloudSL)

	// Step 2: Convert cloud ShadowLink proto back to config
	roundTripConfig := cloudShadowLinkToConfig(cloudSL)
	require.NotNil(t, roundTripConfig)

	// Step 3: Verify the round-trip config matches the original
	// Note: CloudOptions.SourceRedpandaID is not in ShadowLink response (only in create request)
	// so we need to adjust our expectation
	expectedConfig := *originalConfig
	expectedConfig.CloudOptions = &CloudShadowLinkOptions{
		ShadowRedpandaID: originalConfig.CloudOptions.ShadowRedpandaID,
		// SourceRedpandaID is not returned by the API
	}

	require.Equal(t, &expectedConfig, roundTripConfig, "round-trip config should match expected config")
}

// Test reverse enum mapping functions return empty string for
// unknown/unspecified values. These tests validate fallback behavior.
// The round-trip test above covers known values.
func TestAdminScramMechanismToCfg_UnknownValue(t *testing.T) {
	result := adminScramMechanismToCfg(adminv2.ScramMechanism_SCRAM_MECHANISM_UNSPECIFIED)
	require.Equal(t, ScramMechanism(""), result, "unspecified SCRAM mechanism should return empty string")
}

func TestAdminPatternTypeToCfg_UnknownValue(t *testing.T) {
	result := adminPatternTypeToCfg(adminv2.PatternType_PATTERN_TYPE_UNSPECIFIED)
	require.Equal(t, PatternType(""), result, "unspecified pattern type should return empty string")
}

func TestAdminFilterTypeToCfg_UnknownValue(t *testing.T) {
	result := adminFilterTypeToCfg(adminv2.FilterType_FILTER_TYPE_UNSPECIFIED)
	require.Equal(t, FilterType(""), result, "unspecified filter type should return empty string")
}

func TestAdminACLResourceToCfg_UnknownValue(t *testing.T) {
	result := adminACLResourceToCfg(corecommonv1.ACLResource_ACL_RESOURCE_UNSPECIFIED)
	require.Equal(t, ACLResource(""), result, "unspecified ACL resource should return empty string")
}

func TestAdminACLPatternToCfg_UnknownValue(t *testing.T) {
	result := adminACLPatternToCfg(corecommonv1.ACLPattern_ACL_PATTERN_UNSPECIFIED)
	require.Equal(t, ACLPattern(""), result, "unspecified ACL pattern should return empty string")
}

func TestAdminACLOperationToCfg_UnknownValue(t *testing.T) {
	result := adminACLOperationToCfg(corecommonv1.ACLOperation_ACL_OPERATION_UNSPECIFIED)
	require.Equal(t, ACLOperation(""), result, "unspecified ACL operation should return empty string")
}

func TestAdminPermissionTypeToCfg_UnknownValue(t *testing.T) {
	result := adminPermissionTypeToCfg(corecommonv1.ACLPermissionType_ACL_PERMISSION_TYPE_UNSPECIFIED)
	require.Equal(t, ACLPermissionType(""), result, "unspecified permission type should return empty string")
}
