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
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"go.uber.org/zap"

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
func NewClient(ctx context.Context, fs afero.Fs, p *config.RpkProfile, opts ...rpadmin.Opt) (*rpadmin.AdminAPI, error) {
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

	cl, err := rpadmin.NewClient(addrs, tc, auth, p.FromCloud, opts...)
	if err != nil {
		return nil, err
	}
	if msg := licenseFeatureChecks(ctx, fs, cl, p); msg != "" {
		fmt.Fprintln(os.Stderr, msg)
	}
	return cl, nil
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

// licenseFeatureChecks checks if the user is talking to a cluster that has
// enterprise features enabled without a license and returns a message with a
// warning.
func licenseFeatureChecks(ctx context.Context, fs afero.Fs, cl *rpadmin.AdminAPI, p *config.RpkProfile) string {
	var msg string
	// We only do a check if:
	//   1. LicenseCheck == nil: never checked before OR last check was in
	//      violation. (we only save successful responses).
	//   2. LicenseStatus was last checked more than 1 hour ago.
	if p.LicenseCheck == nil || p.LicenseCheck != nil && time.Unix(p.LicenseCheck.LastUpdate, 0).Add(1*time.Hour).Before(time.Now()) {
		resp, err := cl.GetEnterpriseFeatures(ctx)
		if err != nil {
			zap.L().Sugar().Warnf("unable to check licensed enterprise features in the cluster: %v", err)
			return ""
		}
		// We don't write a profile if the config doesn't exist.
		y, exists := p.ActualConfig()
		var licenseCheck *config.LicenseStatusCache
		if resp.Violation {
			var features []string
			for _, f := range resp.Features {
				if f.Enabled {
					features = append(features, f.Name)
				}
			}
			msg = fmt.Sprintf("\nWARNING: The following Enterprise features are being used in your Redpanda cluster: %v. These features require a license. To get a license, contact us at https://www.redpanda.com/contact. For more information, see https://docs.redpanda.com/current/get-started/licenses/#redpanda-enterprise-edition\n", features)
		} else {
			licenseCheck = &config.LicenseStatusCache{
				LastUpdate: time.Now().Unix(),
			}
		}
		if exists && y != nil {
			actProfile := y.Profile(p.Name)
			if actProfile != nil {
				actProfile.LicenseCheck = licenseCheck
				if err := y.Write(fs); err != nil {
					zap.L().Sugar().Warnf("unable to save licensed enterprise features check cache to profile: %v", err)
				}
			}
		}
	}
	return msg
}
