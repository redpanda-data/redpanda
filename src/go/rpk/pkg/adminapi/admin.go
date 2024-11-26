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
	"strings"
	"time"

	"github.com/kr/text"
	mTerm "github.com/moby/term"
	"github.com/redpanda-data/common-go/rpadmin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/oauth"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/oauth/providers/auth0"
	"github.com/spf13/afero"
	"go.uber.org/zap"
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

// TryDecodeMessageFromErr tries to decode the message if it's a
// rpadmin.HTTPResponseError and logs the full error. Otherwise, it returns
// the original error string.
func TryDecodeMessageFromErr(err error) string {
	if err == nil {
		return ""
	}
	if he := (*rpadmin.HTTPResponseError)(nil); errors.As(err, &he) {
		zap.L().Sugar().Debugf("got admin API error: %v", strings.TrimSpace(err.Error()))
		if body, err := he.DecodeGenericErrorBody(); err == nil {
			return body.Message
		}
	}
	return strings.TrimSpace(err.Error())
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
		featResp, err := cl.GetEnterpriseFeatures(ctx)
		if err != nil {
			zap.L().Sugar().Warnf("unable to check licensed enterprise features in the cluster: %v", err)
			return ""
		}
		info, err := cl.GetLicenseInfo(ctx)
		if err != nil {
			zap.L().Sugar().Warnf("unable to check license information: %v", err)
			return ""
		}
		// We don't write a profile if the config doesn't exist.
		y, exists := p.ActualConfig()
		var licenseCheck *config.LicenseStatusCache
		var enabledFeatures []string
		for _, f := range featResp.Features {
			if f.Enabled {
				enabledFeatures = append(enabledFeatures, f.Name)
			}
		}
		// We have 3 types of warnings:
		//  1. WHEN license.type=TRIAL, enterprise feature(s) enabled, AND
		//     expiry in <15 days
		//  2. WHEN license.type=ENTERPRISE and license EXPIRED, AND VIOLATION
		//     (enterprise featured enabled)
		//  3. WHEN there is a VIOLATION (no or expired license and
		//     enterprise(s) features enabled)
		daysLeft, isTrialCheck := isTrialAboutToExpire(info, enabledFeatures)
		switch {
		case isTrialCheck:
			msg = fmt.Sprintf("\nNote: your TRIAL license will expire in %v days. The following Enterprise features are being used in your Redpanda cluster: %v. These features require a license. To request a license, please visit https://redpanda.com/upgrade. For more information, see https://docs.redpanda.com/current/get-started/licenses/#redpanda-enterprise-edition\n", daysLeft, enabledFeatures)
		case isEnterpriseExpired(info, enabledFeatures):
			msg = fmt.Sprintf("\nWARNING: your ENTERPRISE license has expired. The following Enterprise features are being used in your Redpanda cluster: %v. These features require a license. To request a new license, please visit https://support.redpanda.com. For more information, see https://docs.redpanda.com/current/get-started/licenses/#redpanda-enterprise-edition\n", enabledFeatures)
		case featResp.Violation:
			msg = fmt.Sprintf("\nWARNING: The following Enterprise features are being used in your Redpanda cluster: %v. These features require a license. To request a license, please visit http://redpanda.com/upgrade. To try Redpanda Enterprise for 30 days, visit http://redpanda.com/try-enterprise. For more information, see https://docs.redpanda.com/current/get-started/licenses/#redpanda-enterprise-edition\n", enabledFeatures)
		default:
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
	if ws, err := mTerm.GetWinsize(0); err == nil && msg != "" {
		// text.Wrap removes the newline from the text. We add it back.
		msg = "\n" + text.Wrap(msg, int(ws.Width)) + "\n"
	}
	return msg
}

// isTrialAboutToExpire checks if the loaded "free_trial" license will expire in
// less than 15 days and if enterprise features are enabled. It returns the
// number of days remaining until expiration and a boolean indicating whether
// the trial is about to expire.
func isTrialAboutToExpire(info rpadmin.License, enabledFeatures []string) (int, bool) {
	if len(enabledFeatures) > 0 && info.Loaded && strings.EqualFold(info.Properties.Type, "free_trial") {
		ut := time.Unix(info.Properties.Expires, 0)
		daysLeft := int(time.Until(ut).Hours() / 24)

		return daysLeft, daysLeft < 15 && !ut.Before(time.Now())
	}
	return -1, false
}

// isEnterpriseExpired returns true if we have a loaded enterprise license
// that has expired and the user still has enterprise features enabled.
func isEnterpriseExpired(info rpadmin.License, enabledFeatures []string) bool {
	if len(enabledFeatures) > 0 && info.Loaded && strings.EqualFold(info.Properties.Type, "enterprise") {
		ut := time.Unix(info.Properties.Expires, 0)
		return ut.Before(time.Now())
	}
	return false
}
