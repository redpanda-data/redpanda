// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package schemaregistry

import (
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

// loadProfile writes a profile to an in-memory filesystem and loads it back
// through the config system so internal fields are populated.
func loadProfile(t *testing.T, fs afero.Fs, p *config.RpkProfile) *config.RpkProfile {
	t.Helper()
	t.Setenv("HOME", "/")
	p.Name = "test"
	rpkyaml := config.RpkYaml{
		CurrentProfile: "test",
		Version:        7,
		Profiles:       []config.RpkProfile{*p},
	}
	err := rpkyaml.Write(fs)
	require.NoError(t, err)
	y, err := new(config.Params).Load(fs)
	require.NoError(t, err)
	return y.VirtualProfile()
}

func TestNewClient_OAUTHBEARER(t *testing.T) {
	tests := []struct {
		name    string
		profile *config.RpkProfile
		wantErr string
	}{
		{
			name: "OAUTHBEARER with token prefix succeeds",
			profile: &config.RpkProfile{
				SR: config.RpkSchemaRegistryAPI{
					Addresses: []string{"localhost:8081"},
				},
				KafkaAPI: config.RpkKafkaAPI{
					SASL: &config.SASL{
						Password:  "token:my-jwt",
						Mechanism: "OAUTHBEARER",
					},
				},
			},
		},
		{
			name: "OAUTHBEARER with raw token succeeds",
			profile: &config.RpkProfile{
				SR: config.RpkSchemaRegistryAPI{
					Addresses: []string{"localhost:8081"},
				},
				KafkaAPI: config.RpkKafkaAPI{
					SASL: &config.SASL{
						Password:  "my-jwt",
						Mechanism: "OAUTHBEARER",
					},
				},
			},
		},
		{
			name: "OAUTHBEARER case-insensitive",
			profile: &config.RpkProfile{
				SR: config.RpkSchemaRegistryAPI{
					Addresses: []string{"localhost:8081"},
				},
				KafkaAPI: config.RpkKafkaAPI{
					SASL: &config.SASL{
						Password:  "my-jwt",
						Mechanism: "oauthbearer",
					},
				},
			},
		},
		{
			name: "OAUTHBEARER with empty password errors",
			profile: &config.RpkProfile{
				SR: config.RpkSchemaRegistryAPI{
					Addresses: []string{"localhost:8081"},
				},
				KafkaAPI: config.RpkKafkaAPI{
					SASL: &config.SASL{
						Mechanism: "OAUTHBEARER",
					},
				},
			},
			wantErr: "OAUTHBEARER requires a token",
		},
		{
			name: "OAUTHBEARER with token: prefix only errors",
			profile: &config.RpkProfile{
				SR: config.RpkSchemaRegistryAPI{
					Addresses: []string{"localhost:8081"},
				},
				KafkaAPI: config.RpkKafkaAPI{
					SASL: &config.SASL{
						Password:  "token:",
						Mechanism: "OAUTHBEARER",
					},
				},
			},
			wantErr: "OAUTHBEARER requires a token",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := afero.NewMemMapFs()
			p := loadProfile(t, fs, tt.profile)
			cl, err := NewClient(fs, p)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, cl)
		})
	}
}
