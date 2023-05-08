package oauth

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/lestrrat-go/jwx/jwa"
	"github.com/lestrrat-go/jwx/jwt"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cmd/cloud/cloudcfg"
	"github.com/stretchr/testify/require"
)

type MockAuthClient struct {
	audience        string
	mockToken       func(ctx context.Context, cfg cloudcfg.Config) (Token, error)
	mockDeviceToken func(ctx context.Context, cfg cloudcfg.Config, deviceCode string) (Token, error)
	mockDevice      func(ctx context.Context, cfg cloudcfg.Config) (DeviceCode, error)
}

func (cl *MockAuthClient) Audience() string {
	return cl.audience
}

func (cl *MockAuthClient) Token(ctx context.Context, cfg cloudcfg.Config) (Token, error) {
	if cl.mockToken != nil {
		return cl.mockToken(ctx, cfg)
	}
	return Token{}, errors.New("credential flow token call not implemented")
}

func (cl *MockAuthClient) DeviceCode(ctx context.Context, cfg cloudcfg.Config) (DeviceCode, error) {
	if cl.mockDevice != nil {
		return cl.mockDevice(ctx, cfg)
	}
	return DeviceCode{}, errors.New("DeviceCode call not implemented")
}

func (cl *MockAuthClient) DeviceToken(ctx context.Context, cfg cloudcfg.Config, deviceCode string) (Token, error) {
	if cl.mockDeviceToken != nil {
		return cl.mockDeviceToken(ctx, cfg, deviceCode)
	}
	return Token{}, errors.New("device token call not implemented")
}

func (*MockAuthClient) URLOpener(_ string) error {
	return nil
}

func TestClientCredentialFlow(t *testing.T) {
	tests := []struct {
		name   string
		mToken func(ctx context.Context, cfg cloudcfg.Config) (Token, error)
		testFn func(t *testing.T) http.HandlerFunc
		cfg    *cloudcfg.Config
		exp    Token
		expErr bool
	}{
		{
			name: "retrieve token -- validate correct endpoint",
			mToken: func(_ context.Context, _ cloudcfg.Config) (Token, error) {
				return Token{
					AccessToken: "token!",
					ExpiresIn:   100,
					TokenType:   "bearer",
				}, nil
			},
			cfg: &cloudcfg.Config{ClientID: "id", ClientSecret: "secret"},
			exp: Token{
				AccessToken: "token!",
				ExpiresIn:   100,
				TokenType:   "bearer",
			},
		},
		{
			name: "Validate already present token and return the same",
			cfg: &cloudcfg.Config{
				// Expires in 2100-04-05T17:22:27.871Z
				AuthToken:    "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJPbmxpbmUgSldUIEJ1aWxkZXIiLCJpYXQiOjE2ODA3MTUzNDcsImV4cCI6NDExMDYyODk0NywiYXVkIjoidGVzdC1hdWRpZW5jZSIsInN1YiI6InJvZ2dlciIsImF6cCI6ImlkIn0.lYutL1t47HTo1O-zA9QKBjHwtAlgbz3VzV5lT4kXO_g",
				ClientID:     "id",
				AuthAudience: "test-audience",
			},
			exp: Token{AccessToken: "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJPbmxpbmUgSldUIEJ1aWxkZXIiLCJpYXQiOjE2ODA3MTUzNDcsImV4cCI6NDExMDYyODk0NywiYXVkIjoidGVzdC1hdWRpZW5jZSIsInN1YiI6InJvZ2dlciIsImF6cCI6ImlkIn0.lYutL1t47HTo1O-zA9QKBjHwtAlgbz3VzV5lT4kXO_g"},
		},
		{
			name: "Generate new token if stored token is expired",
			mToken: func(_ context.Context, _ cloudcfg.Config) (Token, error) {
				return Token{AccessToken: "newToken"}, nil
			},
			cfg: &cloudcfg.Config{
				// Expired in 2022-11-08T17:22:27.871Z
				AuthToken:    "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJPbmxpbmUgSldUIEJ1aWxkZXIiLCJpYXQiOjE2ODA3MTUzNDcsImV4cCI6MTY2NzkyODE0NywiYXVkIjoidGVzdC1hdWRpZW5jZSIsInN1YiI6InJvZ2dlciIsImF6cCI6ImlkIn0.V54Kg6Zp1rC1ioFb86i8k58PaLlmgyYBCWwulPC9--0",
				ClientID:     "id",
				ClientSecret: "secret",
				AuthAudience: "test-audience",
			},
			exp: Token{AccessToken: "newToken"},
		},
		{
			name: "Generate new token if we dont have client ID",
			mToken: func(_ context.Context, _ cloudcfg.Config) (Token, error) {
				return Token{AccessToken: "newToken"}, nil
			},
			cfg: &cloudcfg.Config{
				AuthToken:    "oldToken", // We generate one new in the absence of clientID since we are not able to validate the token.
				AuthAudience: "test-audience",
				ClientSecret: "secret",
			},
			exp: Token{AccessToken: "newToken"},
		},
		{
			name: "Err if stored token is not valid",
			cfg: &cloudcfg.Config{
				// Expires in 2100-04-05T17:22:27.871Z
				AuthToken:    "not valid",
				ClientID:     "id",
				AuthAudience: "test-audience",
			},
			expErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cl := &MockAuthClient{
				audience:  tt.cfg.AuthAudience,
				mockToken: tt.mToken,
			}
			got, err := ClientCredentialFlow(context.Background(), cl, tt.cfg)
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
		name      string
		mDevice   func(ctx context.Context, cfg cloudcfg.Config) (DeviceCode, error)
		mDevToken func(ctx context.Context, cfg cloudcfg.Config, deviceCode string) (Token, error)
		cfg       *cloudcfg.Config
		exp       Token
		expErr    bool
	}{
		{
			name: "retrieve token",
			mDevice: func(_ context.Context, _ cloudcfg.Config) (DeviceCode, error) {
				return DeviceCode{DeviceCode: "dev", VerificationURLComplete: "https://www.redpanda.com"}, nil
			},
			mDevToken: func(_ context.Context, _ cloudcfg.Config, deviceCode string) (Token, error) {
				if deviceCode != "dev" {
					return Token{}, &TokenResponseError{Err: fmt.Sprintf("unexpected device code %q", deviceCode)}
				}
				return Token{
					AccessToken: "token!",
					ExpiresIn:   100,
					TokenType:   "bearer",
				}, nil
			},
			cfg: &cloudcfg.Config{AuthClientID: "id"},
			exp: Token{
				AccessToken: "token!",
				ExpiresIn:   100,
				TokenType:   "bearer",
			},
		},
		{
			name: "Generate new token if we dont have client ID",
			mDevice: func(_ context.Context, _ cloudcfg.Config) (DeviceCode, error) {
				return DeviceCode{DeviceCode: "dev", VerificationURLComplete: "https://www.redpanda.com"}, nil
			},
			mDevToken: func(_ context.Context, _ cloudcfg.Config, deviceCode string) (Token, error) {
				if deviceCode != "dev" {
					return Token{}, &TokenResponseError{Err: fmt.Sprintf("unexpected device code %q", deviceCode)}
				}
				return Token{AccessToken: "newToken"}, nil
			},
			cfg: &cloudcfg.Config{
				AuthToken:    "oldToken", // We generate one new in the absence of clientID since we are not able to validate the token.
				AuthAudience: "test-audience",
			},
			exp: Token{AccessToken: "newToken"},
		},
		{
			name: "Generate new token if stored token is expired",
			mDevice: func(_ context.Context, _ cloudcfg.Config) (DeviceCode, error) {
				return DeviceCode{DeviceCode: "dev", VerificationURLComplete: "https://www.redpanda.com"}, nil
			},
			mDevToken: func(_ context.Context, _ cloudcfg.Config, deviceCode string) (Token, error) {
				if deviceCode != "dev" {
					return Token{}, &TokenResponseError{Err: fmt.Sprintf("unexpected device code %q", deviceCode)}
				}
				return Token{AccessToken: "newToken"}, nil
			},
			cfg: &cloudcfg.Config{
				// Expired in 2022-11-08T17:22:27.871Z
				AuthToken:    "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJPbmxpbmUgSldUIEJ1aWxkZXIiLCJpYXQiOjE2ODA3MTUzNDcsImV4cCI6MTY2NzkyODE0NywiYXVkIjoidGVzdC1hdWRpZW5jZSIsInN1YiI6InJvZ2dlciIsImF6cCI6ImlkIn0.V54Kg6Zp1rC1ioFb86i8k58PaLlmgyYBCWwulPC9--0",
				AuthClientID: "id",
				AuthAudience: "test-audience",
			},
			exp: Token{AccessToken: "newToken"},
		},
		{
			name: "Validate already present token and return the same",
			cfg: &cloudcfg.Config{
				// Expires in 2100-04-05T17:22:27.871Z
				AuthToken:    "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJPbmxpbmUgSldUIEJ1aWxkZXIiLCJpYXQiOjE2ODA3MTUzNDcsImV4cCI6NDExMDYyODk0NywiYXVkIjoidGVzdC1hdWRpZW5jZSIsInN1YiI6InJvZ2dlciIsImF6cCI6ImlkIn0.lYutL1t47HTo1O-zA9QKBjHwtAlgbz3VzV5lT4kXO_g",
				ClientID:     "id",
				AuthAudience: "test-audience",
			},
			exp: Token{AccessToken: "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJPbmxpbmUgSldUIEJ1aWxkZXIiLCJpYXQiOjE2ODA3MTUzNDcsImV4cCI6NDExMDYyODk0NywiYXVkIjoidGVzdC1hdWRpZW5jZSIsInN1YiI6InJvZ2dlciIsImF6cCI6ImlkIn0.lYutL1t47HTo1O-zA9QKBjHwtAlgbz3VzV5lT4kXO_g"},
		},
		{
			name: "err if the verification url is not valid",
			mDevice: func(_ context.Context, _ cloudcfg.Config) (DeviceCode, error) {
				return DeviceCode{DeviceCode: "dev", VerificationURLComplete: "invalid-url"}, nil
			},
			cfg:    &cloudcfg.Config{ClientID: "id"},
			expErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cl := MockAuthClient{
				audience:        tt.cfg.AuthAudience,
				mockDeviceToken: tt.mDevToken,
				mockDevice:      tt.mDevice,
			}
			got, err := DeviceFlow(context.Background(), &cl, tt.cfg)
			if tt.expErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.exp, got)
		})
	}
}

func TestValidate(t *testing.T) {
	pkey := []byte(`-----BEGIN CERTIFICATE-----
MIICkDCCAfmgAwIBAgIEHoZWwzANBgkqhkiG9w0BAQsFADBfMQswCQYDVQQGEwJO
QTELMAkGA1UECBMCTkExEDAOBgNVBAcTB05vd2hlcmUxDDAKBgNVBAoTA2RldjEP
MA0GA1UEAxMGZGV2IENBMRIwEAYDVQQFEwk1MTIxMjA1MTQwHhcNMjIxMDI0MjE0
MDEwWhcNNDkxMjMxMjM1OTU5WjBiMQswCQYDVQQGEwJOQTELMAkGA1UECBMCTkEx
EDAOBgNVBAcTB05vd2hlcmUxDDAKBgNVBAoTA2RldjESMBAGA1UEAxMJbG9jYWxo
b3N0MRIwEAYDVQQFEwk1MTIxMjA1MTUwgZ8wDQYJKoZIhvcNAQEBBQADgY0AMIGJ
AoGBALRO4Ad4FbK8+eIfJOvhcejrE2rvL56d34ydguX/3mzbB8H79Tbmzv0L1X2N
jvb5zulr9unK/SfQE1OxEtEvtPGBDiZSHLe+xRcsFhtiBAt7ZomFdO95p577FyTk
dBDhIJeIS/Rw10lS6iuGo0LnU0gZfPqNRfPUW4ffnEnggb4zAgMBAAGjVjBUMA4G
A1UdDwEB/wQEAwIFoDATBgNVHSUEDDAKBggrBgEFBQcDAjAMBgNVHRMBAf8EAjAA
MB8GA1UdIwQYMBaAFDMs3r+ghZg8hj5lXaH4MwqL95AfMA0GCSqGSIb3DQEBCwUA
A4GBALRt5Fz1fts+0iTfFd3H+wZuvJWgVYOwVFp6t055mTU014bqKlo1DqDOD4Ud
qaGyeeWSr7npaGhNfb59Mq++Cnk4IDPwNJYVFjw6qt9tSl0fJyBZw+iXvMPPlmxe
+lVIwLOdb9VnWnd1ToyTPgI1S4xiLoXHz2y2MaIgPBkEsk5+
-----END CERTIFICATE-----`)

	const okAud, okID = "ok_aud", "ok_id"

	for _, test := range []struct {
		name string

		audience   string
		clientID   string
		expiry     time.Time
		missingAud bool
		missingExp bool
		expired    bool

		expErrPrefix string
	}{
		{
			name:     "ok",
			audience: okAud,
			clientID: okID,
			expiry:   time.Now().Add(time.Hour),
		},
		{
			name:         "missing audience",
			audience:     okAud,
			clientID:     okID,
			expiry:       time.Now().Add(time.Hour),
			missingAud:   true,
			expErrPrefix: "invalid empty audience",
		},
		{
			name:         "missing expiry",
			audience:     okAud,
			clientID:     okID,
			missingExp:   true,
			expErrPrefix: "invalid non-expiring token",
		},
		{
			name:         "bad audience",
			audience:     "bad",
			clientID:     okID,
			expiry:       time.Now().Add(time.Hour),
			expErrPrefix: "token audience [bad] does not contain our expected audience \"ok_aud\"",
		},
		{
			name:         "bad client id",
			audience:     okAud,
			clientID:     "bad",
			expiry:       time.Now().Add(time.Hour),
			expErrPrefix: "token client id \"bad\" is not our expected client id [\"ok_id\"]",
		},
		{
			name:     "bad expiry",
			audience: okAud,
			clientID: okID,
			expiry:   time.Now().Add(-time.Second),
			expired:  true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			tok := jwt.New()
			tok.Set(jwt.AudienceKey, test.audience)
			tok.Set("azp", test.clientID)
			if !test.missingExp {
				tok.Set(jwt.ExpirationKey, test.expiry) // unix epoch, 0 time
			}

			signed, err := jwt.Sign(tok, jwa.HS256, pkey)
			if err != nil {
				t.Errorf("unexpected error while signing: %v", err)
				return
			}

			useAud := okAud
			if test.missingAud {
				useAud = ""
			}
			expired, err := validateToken(string(signed), useAud, okID)
			require.Equal(t, test.expired, expired)
			if test.expErrPrefix != "" {
				require.Error(t, err)
				require.ErrorContains(t, err, test.expErrPrefix)
				return
			}
			require.NoError(t, err)
		})
	}
}
