package schemaregistry

import (
	"errors"
	"fmt"
	"strings"

	"github.com/redpanda-data/common-go/rpsr"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/netutil"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/oauth/authtoken"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/oauth/providers/auth0"
	"github.com/spf13/afero"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sr"
	"github.com/twmb/franz-go/plugin/kzap"
)

func NewClient(fs afero.Fs, p *config.RpkProfile) (*rpsr.Client, error) {
	api := &p.SR

	d := p.Defaults()
	if len(api.Addresses) == 0 && d.NoDefaultCluster {
		return nil, errors.New("no schema registry hosts specified and rpk.yaml is configured to not use a default cluster")
	}

	urls := p.SR.Addresses
	for i, u := range p.SR.Addresses {
		scheme, _, err := netutil.ParseHostMaybeScheme(u)
		if err != nil {
			return nil, fmt.Errorf("unable to parse your schema registry address %q: %v", u, err)
		}
		switch scheme {
		case "http", "https":
			continue
		case "":
			if p.SR.TLS != nil {
				urls[i] = "https://" + u
			} else {
				urls[i] = "http://" + u
			}
		default:
			return nil, fmt.Errorf("unsupported scheme %q in the schema registry address %q", scheme, u)
		}
	}

	opts := []sr.ClientOpt{
		sr.URLs(urls...),
		sr.UserAgent("rpk"),
		sr.LogFn(wrapKgoLogger(kzap.New(p.Logger()))),
		sr.LogLevel(sr.LogLevelDebug),
	}

	tc, err := api.TLS.Config(fs)
	if err != nil {
		return nil, err
	}
	if tc != nil {
		opts = append(opts, sr.DialTLSConfig(tc))
	}

	switch {
	// OAUTHBEARER must be matched before the HasSASLCredentials() case below:
	// HasSASLCredentials() returns false for OAUTHBEARER today (no username),
	// so the current ordering happens to work, but if that check ever loosens
	// or the cases are reordered, OAUTHBEARER tokens would be sent as a
	// BasicAuth password.
	case p.KafkaAPI.SASL != nil && strings.EqualFold(p.KafkaAPI.SASL.Mechanism, adminapi.OAuthBearer):
		token := adminapi.OAuthBearerToken(p.KafkaAPI.SASL.Password)
		if token == "" {
			return nil, errors.New("OAUTHBEARER requires a token passed via --password (or kafka_api.sasl.password in the profile)")
		}
		opts = append(opts, sr.BearerToken(token))
	case p.HasSASLCredentials() && p.KafkaAPI.SASL.Mechanism != adminapi.CloudOIDC:
		opts = append(opts, sr.BasicAuth(p.KafkaAPI.SASL.User, p.KafkaAPI.SASL.Password))
	case p.KafkaAPI.SASL != nil && p.KafkaAPI.SASL.Mechanism == adminapi.CloudOIDC:
		a := p.CurrentAuth()
		if a == nil || len(a.AuthToken) == 0 {
			return nil, errors.New("no current cloud token found in your profile, please login with 'rpk cloud login'")
		}
		expired, err := authtoken.ValidateToken(
			a.AuthToken,
			auth0.NewClient(p.DevOverrides()).Audience(),
			a.ClientID,
		)
		if err != nil {
			if errors.Is(err, authtoken.ErrMissingToken) {
				return nil, err
			}
			return nil, fmt.Errorf("unable to validate cloud token, please login again using 'rpk cloud login': %v", err)
		}
		if expired {
			return nil, fmt.Errorf("your cloud token has expired, please login again using 'rpk cloud login'")
		}
		opts = append(opts, sr.BearerToken(a.AuthToken))
	default:
		// do nothing
	}
	srCl, err := sr.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	return rpsr.NewClient(srCl)
}

// IsSoftDeleteError checks whether the error is a SoftDeleteError. This error
// occurs when attempting to soft-delete a schema that was already marked as
// soft deleted.
func IsSoftDeleteError(err error) bool {
	errMsg := err.Error()
	return strings.Contains(errMsg, "was soft deleted")
}

func IsSubjectNotFoundError(err error) bool {
	errMsg := err.Error()
	return strings.Contains(errMsg, "Subject") && strings.Contains(errMsg, "not found")
}

// wrapKgoLogger wraps a kgo.Logger to match the sr.LogFn signature.
func wrapKgoLogger(l kgo.Logger) func(int8, string, ...any) {
	return func(lvl int8, msg string, keyvals ...any) {
		l.Log(kgo.LogLevel(lvl), msg, keyvals...)
	}
}
