package oauth

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/stretchr/testify/require"
)

type MockAuthClient struct {
	audience        string
	authClientID    string
	mockToken       func(ctx context.Context, clientID, clientSecret string) (Token, error)
	mockDevice      func(ctx context.Context) (DeviceCode, error)
	mockDeviceToken func(ctx context.Context, deviceCode string) (Token, error)
}

func (cl *MockAuthClient) Audience() string {
	return cl.audience
}

func (cl *MockAuthClient) AuthClientID() string {
	return cl.authClientID
}

func (cl *MockAuthClient) Token(ctx context.Context, clientID, clientSecret string) (Token, error) {
	if cl.mockToken != nil {
		return cl.mockToken(ctx, clientID, clientSecret)
	}
	return Token{}, errors.New("credential flow token call not implemented")
}

func (cl *MockAuthClient) DeviceCode(ctx context.Context) (DeviceCode, error) {
	if cl.mockDevice != nil {
		return cl.mockDevice(ctx)
	}
	return DeviceCode{}, errors.New("DeviceCode call not implemented")
}

func (cl *MockAuthClient) DeviceToken(ctx context.Context, deviceCode string) (Token, error) {
	if cl.mockDeviceToken != nil {
		return cl.mockDeviceToken(ctx, deviceCode)
	}
	return Token{}, errors.New("device token call not implemented")
}

func (*MockAuthClient) URLOpener(_ string) error {
	return nil
}

func TestClientCredentialFlow(t *testing.T) {
	tests := []struct {
		name     string
		mToken   func(ctx context.Context, clientID, clientSecret string) (Token, error)
		testFn   func(*testing.T) http.HandlerFunc
		auth     *config.RpkCloudAuth
		audience string
		exp      Token
		expErr   bool
	}{
		{
			name: "retrieve token -- validate correct endpoint",
			mToken: func(context.Context, string, string) (Token, error) {
				return Token{
					AccessToken: "token!",
					ExpiresIn:   100,
					TokenType:   "bearer",
				}, nil
			},
			auth: &config.RpkCloudAuth{ClientID: "id", ClientSecret: "secret"},
			exp: Token{
				AccessToken: "token!",
				ExpiresIn:   100,
				TokenType:   "bearer",
			},
		},
		{
			name: "Validate already present token and return the same",
			auth: &config.RpkCloudAuth{
				// Expires in 2100-04-05T17:22:27.871Z
				AuthToken: "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJPbmxpbmUgSldUIEJ1aWxkZXIiLCJpYXQiOjE2ODA3MTUzNDcsImV4cCI6NDExMDYyODk0NywiYXVkIjoidGVzdC1hdWRpZW5jZSIsInN1YiI6InJvZ2dlciIsImF6cCI6ImlkIn0.lYutL1t47HTo1O-zA9QKBjHwtAlgbz3VzV5lT4kXO_g",
				ClientID:  "id",
			},
			audience: "test-audience",
			exp:      Token{AccessToken: "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJPbmxpbmUgSldUIEJ1aWxkZXIiLCJpYXQiOjE2ODA3MTUzNDcsImV4cCI6NDExMDYyODk0NywiYXVkIjoidGVzdC1hdWRpZW5jZSIsInN1YiI6InJvZ2dlciIsImF6cCI6ImlkIn0.lYutL1t47HTo1O-zA9QKBjHwtAlgbz3VzV5lT4kXO_g"},
		},
		{
			name: "Generate new token if stored token is expired",
			mToken: func(context.Context, string, string) (Token, error) {
				return Token{AccessToken: "newToken"}, nil
			},
			auth: &config.RpkCloudAuth{
				// Expired in 2022-11-08T17:22:27.871Z
				AuthToken:    "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJPbmxpbmUgSldUIEJ1aWxkZXIiLCJpYXQiOjE2ODA3MTUzNDcsImV4cCI6MTY2NzkyODE0NywiYXVkIjoidGVzdC1hdWRpZW5jZSIsInN1YiI6InJvZ2dlciIsImF6cCI6ImlkIn0.V54Kg6Zp1rC1ioFb86i8k58PaLlmgyYBCWwulPC9--0",
				ClientID:     "id",
				ClientSecret: "secret",
			},
			audience: "test-audience",
			exp:      Token{AccessToken: "newToken"},
		},
		{
			name: "Generate new token if we dont have client ID",
			mToken: func(context.Context, string, string) (Token, error) {
				return Token{AccessToken: "newToken"}, nil
			},
			auth: &config.RpkCloudAuth{
				AuthToken:    "oldToken", // We generate one new in the absence of clientID since we are not able to validate the token.
				ClientSecret: "secret",
			},
			audience: "test-audience",
			exp:      Token{AccessToken: "newToken"},
		},
		{
			name: "Err if stored token is not valid",
			auth: &config.RpkCloudAuth{
				// Expires in 2100-04-05T17:22:27.871Z
				AuthToken: "not valid",
				ClientID:  "id",
			},
			audience: "test-audience",
			expErr:   true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cl := &MockAuthClient{
				audience:  tt.audience,
				mockToken: tt.mToken,
			}
			got, _, err := ClientCredentialFlow(context.Background(), cl, tt.auth, false)
			if tt.expErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.exp, got)
		})
	}
}

func TestDeviceFlow(t *testing.T) {
	tests := []struct {
		name         string
		mDevice      func(context.Context) (DeviceCode, error)
		mDevToken    func(context.Context, string) (Token, error)
		auth         *config.RpkCloudAuth
		audience     string
		authClientID string
		exp          Token
		expErr       bool
	}{
		{
			name: "retrieve token",
			mDevice: func(context.Context) (DeviceCode, error) {
				return DeviceCode{DeviceCode: "dev", VerificationURLComplete: "https://www.redpanda.com"}, nil
			},
			mDevToken: func(_ context.Context, deviceCode string) (Token, error) {
				if deviceCode != "dev" {
					return Token{}, &TokenResponseError{Err: fmt.Sprintf("unexpected device code %q", deviceCode)}
				}
				return Token{
					AccessToken: "token!",
					ExpiresIn:   100,
					TokenType:   "bearer",
				}, nil
			},
			auth:         new(config.RpkCloudAuth),
			authClientID: "id",
			exp: Token{
				AccessToken: "token!",
				ExpiresIn:   100,
				TokenType:   "bearer",
			},
		},
		{
			name: "Generate new token if we dont have client ID",
			mDevice: func(context.Context) (DeviceCode, error) {
				return DeviceCode{DeviceCode: "dev", VerificationURLComplete: "https://www.redpanda.com"}, nil
			},
			mDevToken: func(_ context.Context, deviceCode string) (Token, error) {
				if deviceCode != "dev" {
					return Token{}, &TokenResponseError{Err: fmt.Sprintf("unexpected device code %q", deviceCode)}
				}
				return Token{AccessToken: "newToken"}, nil
			},
			auth: &config.RpkCloudAuth{
				AuthToken: "oldToken", // We generate one new in the absence of clientID since we are not able to validate the token.
			},
			audience: "test-audience",
			exp:      Token{AccessToken: "newToken"},
		},
		{
			name: "Generate new token if stored token is expired",
			mDevice: func(context.Context) (DeviceCode, error) {
				return DeviceCode{DeviceCode: "dev", VerificationURLComplete: "https://www.redpanda.com"}, nil
			},
			mDevToken: func(_ context.Context, deviceCode string) (Token, error) {
				if deviceCode != "dev" {
					return Token{}, &TokenResponseError{Err: fmt.Sprintf("unexpected device code %q", deviceCode)}
				}
				return Token{AccessToken: "newToken"}, nil
			},
			auth: &config.RpkCloudAuth{
				// Expired in 2022-11-08T17:22:27.871Z
				AuthToken: "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJPbmxpbmUgSldUIEJ1aWxkZXIiLCJpYXQiOjE2ODA3MTUzNDcsImV4cCI6MTY2NzkyODE0NywiYXVkIjoidGVzdC1hdWRpZW5jZSIsInN1YiI6InJvZ2dlciIsImF6cCI6ImlkIn0.V54Kg6Zp1rC1ioFb86i8k58PaLlmgyYBCWwulPC9--0",
			},
			audience:     "test-audience",
			authClientID: "id",
			exp:          Token{AccessToken: "newToken"},
		},
		{
			name: "Validate already present token and return the same",
			auth: &config.RpkCloudAuth{
				// Expires in 2100-04-05T17:22:27.871Z
				AuthToken: "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJPbmxpbmUgSldUIEJ1aWxkZXIiLCJpYXQiOjE2ODA3MTUzNDcsImV4cCI6NDExMDYyODk0NywiYXVkIjoidGVzdC1hdWRpZW5jZSIsInN1YiI6InJvZ2dlciIsImF6cCI6ImlkIn0.lYutL1t47HTo1O-zA9QKBjHwtAlgbz3VzV5lT4kXO_g",
				ClientID:  "id",
			},
			audience: "test-audience",
			exp:      Token{AccessToken: "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJPbmxpbmUgSldUIEJ1aWxkZXIiLCJpYXQiOjE2ODA3MTUzNDcsImV4cCI6NDExMDYyODk0NywiYXVkIjoidGVzdC1hdWRpZW5jZSIsInN1YiI6InJvZ2dlciIsImF6cCI6ImlkIn0.lYutL1t47HTo1O-zA9QKBjHwtAlgbz3VzV5lT4kXO_g"},
		},
		{
			name: "err if the verification url is not valid",
			mDevice: func(context.Context) (DeviceCode, error) {
				return DeviceCode{DeviceCode: "dev", VerificationURLComplete: "invalid-url"}, nil
			},
			auth:   &config.RpkCloudAuth{ClientID: "id"},
			expErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cl := MockAuthClient{
				audience:        tt.audience,
				authClientID:    tt.authClientID,
				mockDeviceToken: tt.mDevToken,
				mockDevice:      tt.mDevice,
			}
			got, _, err := DeviceFlow(context.Background(), &cl, tt.auth, false, false)
			if tt.expErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.exp, got)
		})
	}
}
