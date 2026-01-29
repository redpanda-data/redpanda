// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package profile

import (
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestCloudProfileValidator(t *testing.T) {
	tests := []struct {
		name       string
		profile    *config.RpkProfile
		expStatus  validationStatus
		expMessage string
	}{
		{
			name: "cloud profile with cloud URLs",
			profile: &config.RpkProfile{
				FromCloud: true,
				KafkaAPI: config.RpkKafkaAPI{
					Brokers: []string{"seed-abc123.us-east-1.fmc.prd.cloud.redpanda.com:9092"},
				},
			},
			expStatus: statusOK,
		},
		{
			name: "cloud profile with non-cloud URLs",
			profile: &config.RpkProfile{
				FromCloud: true,
				KafkaAPI: config.RpkKafkaAPI{
					Brokers: []string{"localhost:9092"},
				},
			},
			expStatus:  statusWarning,
			expMessage: "Detected Cloud Profile but URLs don't look like Redpanda Cloud; consider 'rpk profile edit'",
		},
		{
			name: "non-cloud profile with cloud URLs",
			profile: &config.RpkProfile{
				FromCloud: false,
				KafkaAPI: config.RpkKafkaAPI{
					Brokers: []string{"seed-abc123.us-east-1.fmc.prd.cloud.redpanda.com:9092"},
				},
			},
			expStatus:  statusWarning,
			expMessage: "URLs look like Redpanda Cloud but from_cloud is set to false; run 'rpk profile set from_cloud=true'",
		},
		{
			name: "non-cloud profile with non-cloud URLs",
			profile: &config.RpkProfile{
				FromCloud: false,
				KafkaAPI: config.RpkKafkaAPI{
					Brokers: []string{"localhost:9092"},
				},
			},
			expStatus:  statusOK,
			expMessage: "Not a Redpanda Cloud profile (skipped)",
		},
		{
			name: "non-cloud profile with empty URLs",
			profile: &config.RpkProfile{
				FromCloud: false,
			},
			expStatus:  statusOK,
			expMessage: "Not a Redpanda Cloud profile (skipped)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := cloudProfileValidator{profile: tt.profile}
			require.True(t, v.ShouldRun(), "cloudProfileValidator should always run")

			result := v.Validate()
			require.Equal(t, "Cloud Settings", result.check)
			require.Equal(t, tt.expStatus, result.status)
			require.Equal(t, tt.expMessage, result.message)
		})
	}
}

func TestAuthReferenceValidator(t *testing.T) {
	tests := []struct {
		name         string
		profile      *config.RpkProfile
		auth         *config.RpkCloudAuth
		expShouldRun bool
		expStatus    validationStatus
		expMessage   string
	}{
		{
			name: "non-cloud profile",
			profile: &config.RpkProfile{
				FromCloud: false,
			},
			expShouldRun: false,
		},
		{
			name: "cloud profile with valid auth reference",
			profile: &config.RpkProfile{
				FromCloud: true,
				CloudCluster: config.RpkCloudCluster{
					AuthOrgID: "org-123",
					AuthKind:  config.CloudAuthSSO,
				},
			},
			auth: &config.RpkCloudAuth{
				OrgID: "org-123",
				Kind:  config.CloudAuthSSO,
			},
			expShouldRun: true,
			expStatus:    statusOK,
		},
		{
			name: "cloud profile missing auth org ID",
			profile: &config.RpkProfile{
				FromCloud: true,
				CloudCluster: config.RpkCloudCluster{
					AuthOrgID: "",
					AuthKind:  config.CloudAuthSSO,
				},
			},
			expShouldRun: true,
			expStatus:    statusError,
			expMessage:   "No auth configured; run 'rpk cloud login' and recreate the profile",
		},
		{
			name: "cloud profile missing auth kind",
			profile: &config.RpkProfile{
				FromCloud: true,
				CloudCluster: config.RpkCloudCluster{
					AuthOrgID: "org-123",
					AuthKind:  "",
				},
			},
			auth:         nil,
			expShouldRun: true,
			expStatus:    statusError,
			expMessage:   "No auth configured; run 'rpk cloud login' and recreate the profile",
		},
		{
			name: "cloud profile references non-existent auth",
			profile: &config.RpkProfile{
				FromCloud: true,
				CloudCluster: config.RpkCloudCluster{
					AuthOrgID: "org-123",
					AuthKind:  config.CloudAuthSSO,
				},
			},
			auth:         nil,
			expShouldRun: true,
			expStatus:    statusError,
			expMessage:   "This profile references non-existent auth (org: org-123, kind: sso); run 'rpk cloud login' and recreate the profile",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := authReferenceValidator{profile: tt.profile, auth: tt.auth}
			require.Equal(t, tt.expShouldRun, v.ShouldRun())

			if !tt.expShouldRun {
				return
			}

			result := v.Validate()
			require.Equal(t, "Auth Reference", result.check)
			require.Equal(t, tt.expStatus, result.status)
			require.Equal(t, tt.expMessage, result.message)
		})
	}
}

func TestAuthKindValidator(t *testing.T) {
	tests := []struct {
		name         string
		auth         *config.RpkCloudAuth
		expShouldRun bool
		expStatus    validationStatus
		expMessage   string
	}{
		{
			name:         "nil auth",
			auth:         nil,
			expShouldRun: false,
		},
		{
			name: "SSO auth without client credentials",
			auth: &config.RpkCloudAuth{
				Kind:      config.CloudAuthSSO,
				AuthToken: "token",
			},
			expShouldRun: true,
			expStatus:    statusOK,
			expMessage:   "",
		},
		{
			name: "SSO auth with client credentials",
			auth: &config.RpkCloudAuth{
				Kind:         config.CloudAuthSSO,
				ClientID:     "client-id",
				ClientSecret: "client-secret",
			},
			expShouldRun: true,
			expStatus:    statusWarning,
			expMessage:   "Kind is 'sso' but client credentials present; consider fixing auth_kind",
		},
		{
			name: "client credentials auth with all fields",
			auth: &config.RpkCloudAuth{
				Kind:         config.CloudAuthClientCredentials,
				ClientID:     "client-id",
				ClientSecret: "client-secret",
			},
			expShouldRun: true,
			expStatus:    statusOK,
			expMessage:   "",
		},
		{
			name: "client credentials auth missing secret",
			auth: &config.RpkCloudAuth{
				Kind:         config.CloudAuthClientCredentials,
				ClientID:     "client-id",
				ClientSecret: "",
			},
			expShouldRun: true,
			expStatus:    statusWarning,
			expMessage:   "Client Secret not stored; token refresh requires re-auth; use --save flag",
		},
		{
			name: "client credentials auth without credentials",
			auth: &config.RpkCloudAuth{
				Kind:         config.CloudAuthClientCredentials,
				ClientID:     "",
				ClientSecret: "",
			},
			expShouldRun: true,
			expStatus:    statusWarning,
			expMessage:   "No client credentials stored; next login will require both client ID and secret",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := authKindValidator{auth: tt.auth}
			require.Equal(t, tt.expShouldRun, v.ShouldRun())

			if !tt.expShouldRun {
				return
			}

			result := v.Validate()
			require.Equal(t, "Auth kind", result.check)
			require.Equal(t, tt.expStatus, result.status)
			require.Equal(t, tt.expMessage, result.message)
		})
	}
}

func TestTokenValidator(t *testing.T) {
	tests := []struct {
		name         string
		auth         *config.RpkCloudAuth
		expShouldRun bool
		expStatus    validationStatus
		expContains  string
	}{
		{
			name:         "nil auth",
			auth:         nil,
			expShouldRun: false,
		},
		{
			name: "missing token",
			auth: &config.RpkCloudAuth{
				AuthToken: "",
			},
			expShouldRun: true,
			expStatus:    statusWarning,
			expContains:  "No token found",
		},
		{
			name: "invalid token format",
			auth: &config.RpkCloudAuth{
				AuthToken: "not-a-valid-jwt",
			},
			expShouldRun: true,
			expStatus:    statusWarning,
			expContains:  "Unable to validate",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := tokenValidator{auth: tt.auth, cfg: &config.Config{}}
			require.Equal(t, tt.expShouldRun, v.ShouldRun())

			if !tt.expShouldRun {
				return
			}

			result := v.Validate()
			require.Equal(t, "Auth Token", result.check)
			require.Equal(t, tt.expStatus, result.status)
			require.Contains(t, result.message, tt.expContains)
		})
	}
}
