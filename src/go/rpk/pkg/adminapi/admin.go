// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// Package adminapi provides a client to interact with Redpanda's admin server.
package adminapi

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/redpanda-data/common-go/rpadmin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/oauth"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/oauth/providers/auth0"
	"github.com/spf13/afero"
)

// GenericErrorBody is the JSON decodable body that is produced by generic error
// handling in the admin server when a seastar http exception is thrown.
type GenericErrorBody struct {
	Message string `json:"message"`
	Code    int    `json:"code"`
}

const (
	ScramSha256 = "SCRAM-SHA-256"
	ScramSha512 = "SCRAM-SHA-512"
	CloudOIDC   = "CLOUD-OIDC"
)

// GetAuth gets the rpadmin.Auth from the rpk profile.
func GetAuth(p *config.RpkProfile) (rpadmin.Auth, error) {
	switch {
	case p.KafkaAPI.SASL != nil && p.KafkaAPI.SASL.Mechanism != CloudOIDC:
		return &rpadmin.BasicAuth{Username: p.KafkaAPI.SASL.User, Password: p.KafkaAPI.SASL.Password}, nil
	case p.KafkaAPI.SASL != nil && p.KafkaAPI.SASL.Mechanism == CloudOIDC:
		a := p.CurrentAuth()
		if a == nil || len(a.AuthToken) == 0 {
			return nil, errors.New("no current auth found, please login with 'rpk cloud login'")
		}
		expired, err := oauth.ValidateToken(
			a.AuthToken,
			auth0.NewClient(p.DevOverrides()).Audience(),
			a.ClientID,
		)
		if err != nil {
			if errors.Is(err, oauth.ErrMissingToken) {
				return nil, err
			}
			return nil, fmt.Errorf("unable to validate cloud token, please login again using 'rpk cloud login': %v", err)
		}
		if expired {
			return nil, fmt.Errorf("your cloud token has expired, please login again using 'rpk cloud login'")
		}
		return &rpadmin.BearerToken{Token: a.AuthToken}, nil
	default:
		return &rpadmin.NopAuth{}, nil
	}
}

// NewClient returns an rpadmin.AdminAPI client that talks to each of the
// addresses in the rpk.admin_api section of the config.
func NewClient(fs afero.Fs, p *config.RpkProfile, opts ...rpadmin.Opt) (*rpadmin.AdminAPI, error) {
	a := &p.AdminAPI

	addrs := a.Addresses
	tc, err := a.TLS.Config(fs)
	if err != nil {
		return nil, fmt.Errorf("unable to create admin api tls config: %v", err)
	}
	auth, err := GetAuth(p)
	if err != nil {
		return nil, err
	}

	return rpadmin.NewClient(addrs, tc, auth, p.FromCloud, opts...)
}

// NewHostClient returns a rpadmin.AdminAPI that talks to the given host, which
// is either an int index into the rpk.admin_api section of the config, or a
// hostname.
func NewHostClient(fs afero.Fs, p *config.RpkProfile, host string) (*rpadmin.AdminAPI, error) {
	if host == "" {
		return nil, errors.New("invalid empty admin host")
	}

	a := &p.AdminAPI
	addrs := a.Addresses
	tc, err := a.TLS.Config(fs)
	if err != nil {
		return nil, fmt.Errorf("unable to create admin api tls config: %v", err)
	}

	i, err := strconv.Atoi(host)
	if err == nil {
		if i < 0 || i >= len(addrs) {
			return nil, fmt.Errorf("admin host %d is out of allowed range [0, %d)", i, len(addrs))
		}
		addrs = []string{addrs[0]}
	} else {
		addrs = []string{host} // trust input is hostname (validate below)
	}

	auth, err := GetAuth(p)
	if err != nil {
		return nil, err
	}
	return rpadmin.NewClient(addrs, tc, auth, p.FromCloud)
}
