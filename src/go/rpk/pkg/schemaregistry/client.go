package schemaregistry

import (
	"errors"
	"fmt"
	"strings"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/net"
	"github.com/spf13/afero"
	"github.com/twmb/franz-go/pkg/sr"
)

func NewClient(fs afero.Fs, p *config.RpkProfile) (*sr.Client, error) {
	api := &p.SR

	d := p.Defaults()
	if len(api.Addresses) == 0 && d.NoDefaultCluster {
		return nil, errors.New("no schema registry hosts specified and rpk.yaml is configured to not use a default cluster")
	}

	urls := p.SR.Addresses
	for i, u := range p.SR.Addresses {
		scheme, _, err := net.ParseHostMaybeScheme(u)
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
	}

	tc, err := api.TLS.Config(fs)
	if err != nil {
		return nil, err
	}
	if tc != nil {
		opts = append(opts, sr.DialTLSConfig(tc))
	}

	if p.HasSASLCredentials() {
		opts = append(opts, sr.BasicAuth(p.KafkaAPI.SASL.User, p.KafkaAPI.SASL.Password))
	}
	return sr.NewClient(opts...)
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
