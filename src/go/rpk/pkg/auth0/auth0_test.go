package auth0

import (
	"strings"
	"testing"
	"time"

	"github.com/lestrrat-go/jwx/jwa"
	"github.com/lestrrat-go/jwx/jwt"
)

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
			name:         "bad expiry",
			audience:     okAud,
			clientID:     okID,
			expiry:       time.Now().Add(-time.Second),
			expErrPrefix: "token is expired as of",
		},

		//
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
			err = ValidateToken(string(signed), useAud, okID)

			if err == nil {
				if test.expErrPrefix != "" {
					t.Errorf("got nil err while expecting error prefix %s", test.expErrPrefix)
				}
			} else if !strings.HasPrefix(err.Error(), test.expErrPrefix) {
				t.Errorf("got err %q, which is missing our expected prefix %q", err, test.expErrPrefix)
			}
		})
	}
}
