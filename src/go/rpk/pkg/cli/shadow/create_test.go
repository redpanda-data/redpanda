package shadow

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestValidateParsedShadowLinkConfig(t *testing.T) {
	date := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name        string
		config      *ShadowLinkConfig
		expectedErr string
	}{
		{
			name:        "nil config",
			config:      nil,
			expectedErr: "provided configuration file generated an empty configuration",
		},
		{
			name: "empty name",
			config: &ShadowLinkConfig{
				Name: "",
				ClientOptions: &ShadowLinkClientOptions{
					BootstrapServers: []string{"broker1:9092"},
				},
			},
			expectedErr: "the Shadow Link name is required",
		},
		{
			name: "no bootstrap servers",
			config: &ShadowLinkConfig{
				Name: "test-link",
				ClientOptions: &ShadowLinkClientOptions{
					BootstrapServers: []string{},
				},
			},
			expectedErr: "at least one bootstrap server is required",
		},
		{
			name: "nil client options with bootstrap servers",
			config: &ShadowLinkConfig{
				Name:          "test-link",
				ClientOptions: &ShadowLinkClientOptions{},
			},
			expectedErr: "at least one bootstrap server is required",
		},
		{
			name: "self-hosted config - omitted client options",
			config: &ShadowLinkConfig{
				Name: "test-link",
				// No ClientOptions block at all; must not panic.
			},
			expectedErr: "at least one bootstrap server is required",
		},
		{
			name: "both TLS file and PEM settings",
			config: &ShadowLinkConfig{
				Name: "test-link",
				ClientOptions: &ShadowLinkClientOptions{
					BootstrapServers: []string{"broker1:9092"},
					TLSSettings: &TLSSettings{
						TLSFileSettings: &TLSFileSettings{
							CAPath: "/path/to/ca",
						},
						TLSPEMSettings: &TLSPEMSettings{
							CA: "pem-content",
						},
					},
				},
			},
			expectedErr: "only one of TLS file settings or PEM settings can be provided",
		},
		{
			name: "multiple StartAt options - latest and earliest",
			config: &ShadowLinkConfig{
				Name: "test-link",
				ClientOptions: &ShadowLinkClientOptions{
					BootstrapServers: []string{"broker1:9092"},
				},
				TopicMetadataSyncOptions: &TopicMetadataSyncOptions{
					StartAtLatest:   &StartAtLatest{},
					StartAtEarliest: &StartAtEarliest{},
				},
			},
			expectedErr: "only one of start_at_latest, start_at_earliest, or start_at_timestamp can be provided",
		},
		{
			name: "multiple StartAt options - earliest and timestamp",
			config: &ShadowLinkConfig{
				Name: "test-link",
				ClientOptions: &ShadowLinkClientOptions{
					BootstrapServers: []string{"broker1:9092"},
				},
				TopicMetadataSyncOptions: &TopicMetadataSyncOptions{
					StartAtEarliest:  &StartAtEarliest{},
					StartAtTimestamp: &date,
				},
			},
			expectedErr: "only one of start_at_latest, start_at_earliest, or start_at_timestamp can be provided",
		},
		{
			name: "multiple StartAt options - latest and timestamp",
			config: &ShadowLinkConfig{
				Name: "test-link",
				ClientOptions: &ShadowLinkClientOptions{
					BootstrapServers: []string{"broker1:9092"},
				},
				TopicMetadataSyncOptions: &TopicMetadataSyncOptions{
					StartAtLatest:    &StartAtLatest{},
					StartAtTimestamp: &date,
				},
			},
			expectedErr: "only one of start_at_latest, start_at_earliest, or start_at_timestamp can be provided",
		},
		{
			name: "all three StartAt options",
			config: &ShadowLinkConfig{
				Name: "test-link",
				ClientOptions: &ShadowLinkClientOptions{
					BootstrapServers: []string{"broker1:9092"},
				},
				TopicMetadataSyncOptions: &TopicMetadataSyncOptions{
					StartAtLatest:    &StartAtLatest{},
					StartAtEarliest:  &StartAtEarliest{},
					StartAtTimestamp: &date,
				},
			},
			expectedErr: "only one of start_at_latest, start_at_earliest, or start_at_timestamp can be provided",
		},
		{
			name: "valid config - minimal",
			config: &ShadowLinkConfig{
				Name: "test-link",
				ClientOptions: &ShadowLinkClientOptions{
					BootstrapServers: []string{"broker1:9092"},
				},
			},
		},
		{
			name: "valid config - with TLS file settings only",
			config: &ShadowLinkConfig{
				Name: "test-link",
				ClientOptions: &ShadowLinkClientOptions{
					BootstrapServers: []string{"broker1:9092"},
					TLSSettings: &TLSSettings{
						TLSFileSettings: &TLSFileSettings{
							CAPath: "/path/to/ca",
						},
					},
				},
			},
		},
		{
			name: "valid config - with TLS PEM settings only",
			config: &ShadowLinkConfig{
				Name: "test-link",
				ClientOptions: &ShadowLinkClientOptions{
					BootstrapServers: []string{"broker1:9092"},
					TLSSettings: &TLSSettings{
						TLSPEMSettings: &TLSPEMSettings{
							CA: "pem-content",
						},
					},
				},
			},
		},
		{
			name: "valid config - with StartAtLatest",
			config: &ShadowLinkConfig{
				Name: "test-link",
				ClientOptions: &ShadowLinkClientOptions{
					BootstrapServers: []string{"broker1:9092"},
				},
				TopicMetadataSyncOptions: &TopicMetadataSyncOptions{
					StartAtLatest: &StartAtLatest{},
				},
			},
		},
		{
			name: "valid config - with StartAtEarliest",
			config: &ShadowLinkConfig{
				Name: "test-link",
				ClientOptions: &ShadowLinkClientOptions{
					BootstrapServers: []string{"broker1:9092"},
				},
				TopicMetadataSyncOptions: &TopicMetadataSyncOptions{
					StartAtEarliest: &StartAtEarliest{},
				},
			},
		},
		{
			name: "valid config - with StartAtTimestamp",
			config: &ShadowLinkConfig{
				Name: "test-link",
				ClientOptions: &ShadowLinkClientOptions{
					BootstrapServers: []string{"broker1:9092"},
				},
				TopicMetadataSyncOptions: &TopicMetadataSyncOptions{
					StartAtTimestamp: &date,
				},
			},
		},
		{
			name: "valid config - with multiple bootstrap servers",
			config: &ShadowLinkConfig{
				Name: "test-link",
				ClientOptions: &ShadowLinkClientOptions{
					BootstrapServers: []string{"broker1:9092", "broker2:9092", "broker3:9092"},
				},
			},
		},
		{
			name: "valid config - TopicMetadataSyncOptions without StartAt fields",
			config: &ShadowLinkConfig{
				Name: "test-link",
				ClientOptions: &ShadowLinkClientOptions{
					BootstrapServers: []string{"broker1:9092"},
				},
				TopicMetadataSyncOptions: &TopicMetadataSyncOptions{
					Interval: 45 * time.Second,
				},
			},
		},
		// Schema registry validation tests
		{
			name: "both schema registry shadowing modes set",
			config: &ShadowLinkConfig{
				Name: "test-link",
				ClientOptions: &ShadowLinkClientOptions{
					BootstrapServers: []string{"broker1:9092"},
				},
				SchemaRegistrySyncOptions: &SchemaRegistrySyncOptions{
					ShadowSchemaRegistryTopic: &ShadowSchemaRegistryTopic{},
					ShadowSchemaRegistryAPI: &ShadowSchemaRegistryAPI{
						SourceURL: "https://source-sr:8081",
					},
				},
			},
			expectedErr: "only one of shadow_schema_registry_topic or shadow_schema_registry_api can be provided",
		},
		{
			name: "schema registry API - both TLS file and PEM settings",
			config: &ShadowLinkConfig{
				Name: "test-link",
				ClientOptions: &ShadowLinkClientOptions{
					BootstrapServers: []string{"broker1:9092"},
				},
				SchemaRegistrySyncOptions: &SchemaRegistrySyncOptions{
					ShadowSchemaRegistryAPI: &ShadowSchemaRegistryAPI{
						SourceURL: "https://source-sr:8081",
						TLSSettings: &TLSSettings{
							TLSFileSettings: &TLSFileSettings{CAPath: "/path/to/ca"},
							TLSPEMSettings:  &TLSPEMSettings{CA: "pem-content"},
						},
					},
				},
			},
			expectedErr: "only one of schema registry API TLS file settings or PEM settings can be provided",
		},
		{
			name: "schema registry API - both destination mappings set",
			config: &ShadowLinkConfig{
				Name: "test-link",
				ClientOptions: &ShadowLinkClientOptions{
					BootstrapServers: []string{"broker1:9092"},
				},
				SchemaRegistrySyncOptions: &SchemaRegistrySyncOptions{
					ShadowSchemaRegistryAPI: &ShadowSchemaRegistryAPI{
						SourceURL: "https://source-sr:8081",
						Destination: &SchemaRegistryContextDestination{
							Identity: &SchemaRegistryIdentityContextMapping{},
							Exact: &SchemaRegistryExactContextMappings{
								Mappings: []*SchemaRegistryContextMap{
									{Source: ".", Destination: ".shadow"},
								},
							},
						},
					},
				},
			},
			expectedErr: "only one of schema registry destination identity or exact can be provided",
		},
		{
			name: "valid config - schema registry API mode",
			config: &ShadowLinkConfig{
				Name: "test-link",
				ClientOptions: &ShadowLinkClientOptions{
					BootstrapServers: []string{"broker1:9092"},
				},
				SchemaRegistrySyncOptions: &SchemaRegistrySyncOptions{
					ShadowSchemaRegistryAPI: &ShadowSchemaRegistryAPI{
						SourceURL: "https://source-sr:8081",
						AuthOptions: &SchemaRegistryAuthOptions{
							Basic: &HTTPBasicAuthOptions{Username: "u", Password: "p"},
						},
						Destination: &SchemaRegistryContextDestination{
							Identity: &SchemaRegistryIdentityContextMapping{},
						},
					},
				},
			},
		},
		// Cloud-specific validation tests
		{
			name: "cloud config - missing shadow_redpanda_id",
			config: &ShadowLinkConfig{
				Name:         "test-link",
				CloudOptions: &CloudShadowLinkOptions{}, // Missing ShadowRedpandaID
				ClientOptions: &ShadowLinkClientOptions{
					BootstrapServers: []string{"broker1:9092"},
				},
			},
			expectedErr: "shadow_redpanda_id is required in cloud options",
		},
		{
			name: "cloud config - same source and shadow redpanda IDs",
			config: &ShadowLinkConfig{
				Name: "test-link",
				CloudOptions: &CloudShadowLinkOptions{
					ShadowRedpandaID: "same-cluster-id",
					SourceRedpandaID: "same-cluster-id",
				},
				ClientOptions: &ShadowLinkClientOptions{
					BootstrapServers: []string{"broker1:9092"},
				},
			},
			expectedErr: "shadow_redpanda_id and source_redpanda_id cannot be the same",
		},
		{
			name: "cloud config - TLS file settings not allowed",
			config: &ShadowLinkConfig{
				Name: "test-link",
				CloudOptions: &CloudShadowLinkOptions{
					ShadowRedpandaID: "shadow-cluster",
				},
				ClientOptions: &ShadowLinkClientOptions{
					BootstrapServers: []string{"broker1:9092"},
					TLSSettings: &TLSSettings{
						Enabled: true,
						TLSFileSettings: &TLSFileSettings{
							CAPath: "/path/to/ca.crt",
						},
					},
				},
			},
			expectedErr: "TLS file settings are not supported when using cloud options",
		},
		{
			name: "cloud config - plain password not allowed",
			config: &ShadowLinkConfig{
				Name: "test-link",
				CloudOptions: &CloudShadowLinkOptions{
					ShadowRedpandaID: "shadow-cluster",
				},
				ClientOptions: &ShadowLinkClientOptions{
					BootstrapServers: []string{"broker1:9092"},
					AuthenticationConfiguration: &AuthenticationConfiguration{
						ScramConfiguration: &ScramConfiguration{
							Username:       "user",
							Password:       "plain-password-not-allowed",
							ScramMechanism: ScramMechanismScramSha256,
						},
					},
				},
			},
			expectedErr: "cloud shadow links don't support plain passwords",
		},
		{
			name: "cloud config - plain tls key not allowed",
			config: &ShadowLinkConfig{
				Name: "test-link",
				CloudOptions: &CloudShadowLinkOptions{
					ShadowRedpandaID: "shadow-cluster",
				},
				ClientOptions: &ShadowLinkClientOptions{
					BootstrapServers: []string{"broker1:9092"},
					TLSSettings: &TLSSettings{
						Enabled: true,
						TLSPEMSettings: &TLSPEMSettings{
							Key: "-----BEGIN CERTIFICATE-----\n...\n-----END CERTIFICATE-----",
						},
					},
				},
			},
			expectedErr: "cloud shadow links don't support plain TLS keys",
		},
		{
			name: "cloud config - schema registry API plain password not allowed",
			config: &ShadowLinkConfig{
				Name: "test-link",
				CloudOptions: &CloudShadowLinkOptions{
					ShadowRedpandaID: "shadow-cluster",
				},
				ClientOptions: &ShadowLinkClientOptions{
					BootstrapServers: []string{"broker1:9092"},
				},
				SchemaRegistrySyncOptions: &SchemaRegistrySyncOptions{
					ShadowSchemaRegistryAPI: &ShadowSchemaRegistryAPI{
						SourceURL: "https://source-sr:8081",
						AuthOptions: &SchemaRegistryAuthOptions{
							Basic: &HTTPBasicAuthOptions{
								Username: "user",
								Password: "plain-password-not-allowed",
							},
						},
					},
				},
			},
			expectedErr: "cloud shadow links don't support plain passwords",
		},
		{
			name: "cloud config - schema registry API plain tls key not allowed",
			config: &ShadowLinkConfig{
				Name: "test-link",
				CloudOptions: &CloudShadowLinkOptions{
					ShadowRedpandaID: "shadow-cluster",
				},
				ClientOptions: &ShadowLinkClientOptions{
					BootstrapServers: []string{"broker1:9092"},
				},
				SchemaRegistrySyncOptions: &SchemaRegistrySyncOptions{
					ShadowSchemaRegistryAPI: &ShadowSchemaRegistryAPI{
						SourceURL: "https://source-sr:8081",
						TLSSettings: &TLSSettings{
							Enabled: true,
							TLSPEMSettings: &TLSPEMSettings{
								Key: "-----BEGIN CERTIFICATE-----\n...\n-----END CERTIFICATE-----",
							},
						},
					},
				},
			},
			expectedErr: "cloud shadow links don't support plain TLS keys",
		},
		{
			name: "cloud config - schema registry API TLS file settings not allowed",
			config: &ShadowLinkConfig{
				Name: "test-link",
				CloudOptions: &CloudShadowLinkOptions{
					ShadowRedpandaID: "shadow-cluster",
				},
				ClientOptions: &ShadowLinkClientOptions{
					BootstrapServers: []string{"broker1:9092"},
				},
				SchemaRegistrySyncOptions: &SchemaRegistrySyncOptions{
					ShadowSchemaRegistryAPI: &ShadowSchemaRegistryAPI{
						SourceURL: "https://source-sr:8081",
						TLSSettings: &TLSSettings{
							Enabled:         true,
							TLSFileSettings: &TLSFileSettings{CAPath: "/path/to/ca.crt"},
						},
					},
				},
			},
			expectedErr: "schema registry API TLS file settings are not supported when using cloud options",
		},
		{
			name: "cloud config - schema registry API plain password with omitted client options",
			config: &ShadowLinkConfig{
				Name: "test-link",
				CloudOptions: &CloudShadowLinkOptions{
					ShadowRedpandaID: "shadow-cluster",
				},
				// No ClientOptions block; the SR-API secret check must still run.
				SchemaRegistrySyncOptions: &SchemaRegistrySyncOptions{
					ShadowSchemaRegistryAPI: &ShadowSchemaRegistryAPI{
						SourceURL: "https://source-sr:8081",
						AuthOptions: &SchemaRegistryAuthOptions{
							Basic: &HTTPBasicAuthOptions{
								Username: "user",
								Password: "plain-password-not-allowed",
							},
						},
					},
				},
			},
			expectedErr: "cloud shadow links don't support plain passwords",
		},
		{
			name: "valid cloud config - schema registry API with secrets",
			config: &ShadowLinkConfig{
				Name: "test-link",
				CloudOptions: &CloudShadowLinkOptions{
					ShadowRedpandaID: "shadow-cluster",
				},
				ClientOptions: &ShadowLinkClientOptions{
					BootstrapServers: []string{"broker1:9092"},
				},
				SchemaRegistrySyncOptions: &SchemaRegistrySyncOptions{
					ShadowSchemaRegistryAPI: &ShadowSchemaRegistryAPI{
						SourceURL: "https://source-sr:8081",
						AuthOptions: &SchemaRegistryAuthOptions{
							Basic: &HTTPBasicAuthOptions{
								Username: "user",
								Password: "${secrets.SR_PASSWORD}",
							},
						},
						TLSSettings: &TLSSettings{
							Enabled: true,
							TLSPEMSettings: &TLSPEMSettings{
								CA:  "-----BEGIN CERTIFICATE-----\n...\n-----END CERTIFICATE-----",
								Key: "${secrets.SR_TLS_KEY}",
							},
						},
					},
				},
			},
		},
		{
			name: "valid cloud config - with secrets store password",
			config: &ShadowLinkConfig{
				Name: "test-link",
				CloudOptions: &CloudShadowLinkOptions{
					ShadowRedpandaID: "shadow-cluster",
					SourceRedpandaID: "source-cluster",
				},
				ClientOptions: &ShadowLinkClientOptions{
					BootstrapServers: []string{"broker1:9092"},
					AuthenticationConfiguration: &AuthenticationConfiguration{
						ScramConfiguration: &ScramConfiguration{
							Username:       "user",
							Password:       "${secrets.MY_SECRET_PASSWORD}",
							ScramMechanism: ScramMechanismScramSha256,
						},
					},
				},
			},
		},
		{
			name: "valid cloud config - with TLS PEM settings",
			config: &ShadowLinkConfig{
				Name: "test-link",
				CloudOptions: &CloudShadowLinkOptions{
					ShadowRedpandaID: "shadow-cluster",
				},
				ClientOptions: &ShadowLinkClientOptions{
					BootstrapServers: []string{"broker1:9092"},
					TLSSettings: &TLSSettings{
						Enabled: true,
						TLSPEMSettings: &TLSPEMSettings{
							CA:   "-----BEGIN CERTIFICATE-----\n...\n-----END CERTIFICATE-----",
							Key:  "${secrets.MY_TLS_KEY}",
							Cert: "-----BEGIN CERTIFICATE-----\n...\n-----END CERTIFICATE-----",
						},
					},
				},
			},
		},
		{
			name: "valid cloud config - no bootstrap servers required",
			config: &ShadowLinkConfig{
				Name: "test-link",
				CloudOptions: &CloudShadowLinkOptions{
					ShadowRedpandaID: "shadow-cluster",
					SourceRedpandaID: "source-cluster",
				},
				ClientOptions: &ShadowLinkClientOptions{
					// No bootstrap servers - valid for cloud
				},
			},
		},
		{
			name: "valid cloud config - minimal",
			config: &ShadowLinkConfig{
				Name: "test-link",
				CloudOptions: &CloudShadowLinkOptions{
					ShadowRedpandaID: "shadow-cluster",
				},
				ClientOptions: &ShadowLinkClientOptions{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateParsedShadowLinkConfig(tt.config)
			if tt.expectedErr == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.expectedErr)
			}
		})
	}
}
