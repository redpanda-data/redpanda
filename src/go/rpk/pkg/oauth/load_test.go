package oauth

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

func init() {
	inTests = true
}

func TestLoadFlow(t *testing.T) {
	tests := []struct {
		name         string
		clientID     string
		clientSecret string
		authClientID string
		mToken       func(ctx context.Context, clientID, clientSecret string) (Token, error)
		mDevice      func(ctx context.Context) (DeviceCode, error)
		mDeviceToken func(ctx context.Context, deviceCode string) (Token, error)
		exp          string
		expKind      string
		expErr       bool
	}{
		{
			name:         "get token with client credentials",
			clientSecret: "secret",
			clientID:     "id",
			mToken: func(_ context.Context, _, _ string) (Token, error) {
				return Token{AccessToken: "success-credential"}, nil
			},
			exp:     "success-credential",
			expKind: config.CloudAuthClientCredentials,
		},
		{
			name:         "get token with device flow",
			authClientID: "id",
			mDevice: func(context.Context) (DeviceCode, error) {
				return DeviceCode{DeviceCode: "dev", VerificationURLComplete: "https://www.redpanda.com"}, nil
			},
			mDeviceToken: func(context.Context, string) (Token, error) {
				return Token{AccessToken: "success-device"}, nil
			},
			exp:     "success-device",
			expKind: config.CloudAuthSSO,
		},
		{
			name:         "choose client credentials over device if credentials are provided",
			clientSecret: "secret",
			clientID:     "id",
			mToken: func(context.Context, string, string) (Token, error) {
				return Token{AccessToken: "success-credential"}, nil
			},
			mDevice: func(context.Context) (DeviceCode, error) {
				return DeviceCode{}, errors.New("unexpected device call")
			},
			mDeviceToken: func(context.Context, string) (Token, error) {
				return Token{}, errors.New("unexpected device token call")
			},
			exp:     "success-credential",
			expKind: config.CloudAuthClientCredentials,
		},
		{
			name:     "errs if a provider err",
			clientID: "id",
			mDevice: func(context.Context) (DeviceCode, error) {
				return DeviceCode{}, errors.New("some err")
			},
			expErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := afero.NewMemMapFs()
			t.Setenv("HOME", "/tmp")
			m := MockAuthClient{
				audience:        "not-tested",
				authClientID:    tt.authClientID,
				mockToken:       tt.mToken,
				mockDeviceToken: tt.mDeviceToken,
				mockDevice:      tt.mDevice,
			}
			p := &config.Params{
				FlagOverrides: []string{
					"cloud.client_id=" + tt.clientID,
					"cloud.client_secret=" + tt.clientSecret,
				},
			}
			cfg, err := p.Load(fs)
			require.NoError(t, err)
			authVir, _, _, _, err := LoadFlow(context.Background(), fs, cfg, &m, false, false, "")
			if tt.expErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			// Assert that we got the right token.
			require.Equal(t, tt.exp, authVir.AuthToken)

			// Now check if it got written to disk.
			y := cfg.VirtualRpkYaml()
			file, err := afero.ReadFile(fs, y.FileLocation())
			require.NoError(t, err)
			var hasClientID bool
			if tt.clientID != "" || tt.authClientID != "" {
				if tt.authClientID != "" {
					tt.clientID = tt.authClientID
				}
				hasClientID = true
			}

			expFile := fmt.Sprintf(`version: 6
globals:
    prompt: ""
    no_default_cluster: false
    command_timeout: 0s
    dial_timeout: 0s
    request_timeout_overhead: 0s
    retry_timeout: 0s
    fetch_max_wait: 0s
    kafka_protocol_request_client_id: ""
current_profile: ""
current_cloud_auth_org_id: no-url-org-id
current_cloud_auth_kind: %[1]s
profiles: []
cloud_auth:
    - name: no-url-org-id-%[1]s no-url-org
      organization: no-url-org
      org_id: no-url-org-id
      kind: %[1]s
      auth_token: %[2]s`, tt.expKind, authVir.AuthToken)
			if hasClientID {
				expFile += fmt.Sprintf(`
      client_id: %s`, tt.clientID)
			}
			expFile += "\n"

			require.Equal(t, expFile, string(file))
		})
	}
}
