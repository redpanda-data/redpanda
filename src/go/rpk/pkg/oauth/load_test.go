package oauth

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cloud/cloudcfg"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

func TestLoadFlow(t *testing.T) {
	tests := []struct {
		name         string
		cfg          *cloudcfg.Config
		mToken       func(ctx context.Context, cfg cloudcfg.Config) (Token, error)
		mDevice      func(ctx context.Context, cfg cloudcfg.Config) (DeviceCode, error)
		mDeviceToken func(ctx context.Context, cfg cloudcfg.Config, deviceCode string) (Token, error)
		exp          string
		expErr       bool
	}{
		{
			name: "get token with client credentials",
			cfg: &cloudcfg.Config{
				ClientSecret: "secret",
				ClientID:     "id",
			},
			mToken: func(_ context.Context, _ cloudcfg.Config) (Token, error) {
				return Token{AccessToken: "success-credential"}, nil
			},
			exp: "success-credential",
		},
		{
			name: "get token with device flow",
			cfg: &cloudcfg.Config{
				AuthClientID: "id",
			},
			mDevice: func(_ context.Context, _ cloudcfg.Config) (DeviceCode, error) {
				return DeviceCode{DeviceCode: "dev", VerificationURLComplete: "https://www.redpanda.com"}, nil
			},
			mDeviceToken: func(_ context.Context, _ cloudcfg.Config, _ string) (Token, error) {
				return Token{AccessToken: "success-device"}, nil
			},
			exp: "success-device",
		},
		{
			name: "choose client credentials over device if credentials are provided",
			cfg: &cloudcfg.Config{
				ClientSecret: "secret",
				ClientID:     "id",
			},
			mToken: func(_ context.Context, _ cloudcfg.Config) (Token, error) {
				return Token{AccessToken: "success-credential"}, nil
			},
			mDevice: func(_ context.Context, _ cloudcfg.Config) (DeviceCode, error) {
				return DeviceCode{}, errors.New("unexpected device call")
			},
			mDeviceToken: func(_ context.Context, _ cloudcfg.Config, _ string) (Token, error) {
				return Token{}, errors.New("unexpected device token call")
			},
			exp: "success-credential",
		},
		{
			name: "errs if a provider err",
			cfg: &cloudcfg.Config{
				ClientID: "id",
			},
			mDevice: func(_ context.Context, _ cloudcfg.Config) (DeviceCode, error) {
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
				mockToken:       tt.mToken,
				mockDeviceToken: tt.mDeviceToken,
				mockDevice:      tt.mDevice,
			}
			gotToken, err := LoadFlow(context.Background(), fs, tt.cfg, &m)
			if tt.expErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			// Assert that we got the right token.
			require.Equal(t, tt.exp, gotToken)

			// Now check if it got written to disk.
			dir, err := os.UserConfigDir()
			require.NoError(t, err)
			fileLocation := filepath.Join(dir, "rpk", "__cloud.yaml")

			file, err := afero.ReadFile(fs, fileLocation)
			require.NoError(t, err)
			expFile := fmt.Sprintf("client_id: %s\nauth_token: %s\n", tt.cfg.ClientID, gotToken)
			require.Equal(t, string(file), expFile)
		})
	}
}
