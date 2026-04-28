// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package adminapi

import (
	"testing"

	"github.com/redpanda-data/common-go/rpadmin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestGetAuth(t *testing.T) {
	tests := []struct {
		name    string
		profile *config.RpkProfile
		wantTyp rpadmin.Auth
		wantErr string
	}{
		{
			name:    "no SASL returns NopAuth",
			profile: &config.RpkProfile{},
			wantTyp: &rpadmin.NopAuth{},
		},
		{
			name: "SCRAM-SHA-256 returns BasicAuth",
			profile: &config.RpkProfile{
				KafkaAPI: config.RpkKafkaAPI{
					SASL: &config.SASL{
						User:      "admin",
						Password:  "secret",
						Mechanism: "SCRAM-SHA-256",
					},
				},
			},
			wantTyp: &rpadmin.BasicAuth{Username: "admin", Password: "secret"},
		},
		{
			name: "OAUTHBEARER with token prefix returns BearerToken",
			profile: &config.RpkProfile{
				KafkaAPI: config.RpkKafkaAPI{
					SASL: &config.SASL{
						Password:  "token:my-jwt-token",
						Mechanism: "OAUTHBEARER",
					},
				},
			},
			wantTyp: &rpadmin.BearerToken{Token: "my-jwt-token"},
		},
		{
			name: "OAUTHBEARER with raw token returns BearerToken",
			profile: &config.RpkProfile{
				KafkaAPI: config.RpkKafkaAPI{
					SASL: &config.SASL{
						Password:  "my-jwt-token",
						Mechanism: "OAUTHBEARER",
					},
				},
			},
			wantTyp: &rpadmin.BearerToken{Token: "my-jwt-token"},
		},
		{
			name: "OAUTHBEARER case-insensitive",
			profile: &config.RpkProfile{
				KafkaAPI: config.RpkKafkaAPI{
					SASL: &config.SASL{
						Password:  "my-jwt-token",
						Mechanism: "oauthbearer",
					},
				},
			},
			wantTyp: &rpadmin.BearerToken{Token: "my-jwt-token"},
		},
		{
			name: "OAUTHBEARER with empty password errors",
			profile: &config.RpkProfile{
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
			got, err := GetAuth(tt.profile)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.IsType(t, tt.wantTyp, got)
			require.Equal(t, tt.wantTyp, got)
		})
	}
}

func TestOAuthBearerToken(t *testing.T) {
	tests := []struct {
		name     string
		password string
		want     string
	}{
		{
			name:     "token prefix stripped",
			password: "token:eyJhbGciOiJSUzI1NiJ9.payload.sig",
			want:     "eyJhbGciOiJSUzI1NiJ9.payload.sig",
		},
		{
			name:     "raw token returned as-is",
			password: "eyJhbGciOiJSUzI1NiJ9.payload.sig",
			want:     "eyJhbGciOiJSUzI1NiJ9.payload.sig",
		},
		{
			name:     "empty password returns empty",
			password: "",
			want:     "",
		},
		{
			name:     "token prefix only returns empty",
			password: "token:",
			want:     "",
		},
		{
			name:     "token prefix is case-sensitive",
			password: "Token:abc",
			want:     "Token:abc",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := OAuthBearerToken(tt.password)
			require.Equal(t, tt.want, got)
		})
	}
}
