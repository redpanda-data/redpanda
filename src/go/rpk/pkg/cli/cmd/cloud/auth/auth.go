// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// Package auth contain specific configuration and handlers for the
// authentication with the Redpanda cloud.
package auth

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/AlecAivazis/survey/v2"
	"github.com/pkg/browser"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/auth0"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cmd/cloud/cloudcfg"
	rpkos "github.com/redpanda-data/redpanda/src/go/rpk/pkg/os"
	"github.com/spf13/afero"
)

// The auth0 endpoint information to get dev tokens from.
var prodAuth0Endpoint = auth0.Endpoint{
	URL:      "https://auth.prd.cloud.redpanda.com",
	Audience: "cloudv2-production.redpanda.cloud",
}

// BadClientTokenError is returned when the client ID is invalid or some other
// error occurs. This can be used as a hint that the client ID needs to be
// cleared as well.
type BadClientTokenError struct {
	Err error
}

func (e *BadClientTokenError) Error() string {
	return fmt.Sprintf("invalid client token: %v", e.Err)
}

func DeviceAuthorizationFlow(ctx context.Context, cfg *cloudcfg.Config) (token string, rerr error) {
	auth0Endpoint := auth0.Endpoint{
		URL:      cfg.CloudURL,
		Audience: cfg.AuthAudience,
	}

	if auth0Endpoint.URL == "" {
		auth0Endpoint = prodAuth0Endpoint
	}
	defer func() {
		if token != "" {
			cfg.AuthToken = token
		}
	}()
	auth0Client := auth0.NewClient(auth0Endpoint)
	resp, err := auth0Client.InitDeviceAuthorization(ctx, cfg.AuthClientID)
	if err != nil {
		return "", fmt.Errorf("unable to start authorization flow: %v", err)
	}
	err = browser.OpenURL(resp.VerificationURLComplete)
	if err != nil {
		return "", fmt.Errorf("unable to open the web browser: %v", err)
	}
	tokenInterval := 5
	return auth0Client.WaitForDeviceToken(ctx, resp.DeviceCode, cfg.AuthClientID, tokenInterval)
}

// LoadFlow loads or creates a config at default path, and validates and
// refreshes or creates an auth0 token using the given auth0 parameters.
//
// This function is expected to be called at the start of most commands.
func LoadFlow(ctx context.Context, fs afero.Fs, cfg *cloudcfg.Config) (token string, rerr error) {
	auth0Endpoint := auth0.Endpoint{
		URL:      cfg.AuthURL,
		Audience: cfg.AuthAudience,
	}

	if auth0Endpoint.URL == "" {
		auth0Endpoint = prodAuth0Endpoint
	}

	// We want to avoid creating a root owned file. If the file exists, we
	// just chmod with rpkos.ReplaceFile and keep old perms even with sudo.
	// If the file does not exist, we will always be creating it to write
	// the token, so we fail if we are running with sudo.
	if !cfg.Exists() && rpkos.IsRunningSudo() {
		return "", fmt.Errorf("detected rpk is running with sudo; please execute this command without sudo to avoid saving the cloud configuration as a root owned file")
	}

	// If we have to prompt, then we will save the client id and secret to
	// the file as well: we do not want the user to repeat input when they
	// run this flow again.
	prompt := !cfg.HasClientCredentials()
	if prompt {
		var err error
		if cfg.ClientID, cfg.ClientSecret, err = promptClientCfg(); err != nil {
			return "", err
		}
	}
	defer func() {
		if rerr == nil {
			if prompt { // see above
				rerr = cfg.SaveAll(fs)
			} else {
				rerr = cfg.SaveToken(fs)
			}
		}
	}()

	if cfg.AuthToken != "" {
		expired, err := validateToken(auth0Endpoint, cfg.AuthToken, cfg.ClientID) //nolint:contextcheck // jwx/jwt package uses ctx.Background in a function down the stream
		if err != nil {
			return "", &BadClientTokenError{err}
		}
		if !expired {
			return cfg.AuthToken, nil
		}
	}

	resp, err := auth0.NewClient(auth0Endpoint).GetToken(ctx, cfg.ClientID, cfg.ClientSecret)
	if err != nil {
		return "", fmt.Errorf("unable to retrieve a cloud token: %v", err)
	}
	cfg.AuthToken = resp.AccessToken

	return cfg.AuthToken, nil
}

// validateToken validates a token and returns whether a refresh is needed and
// notifies the user if it does.
func validateToken(auth0Endpoint auth0.Endpoint, token, clientID string) (expired bool, err error) {
	err = auth0.ValidateToken(token, auth0Endpoint.Audience, clientID)
	if err == nil {
		return false, nil
	}
	if ee := (*auth0.ExpiredError)(nil); errors.As(err, &ee) {
		return true, nil
	}
	return false, err
}

func promptClientCfg() (clientID, clientSecret string, err error) {
	fmt.Println("What is your client ID and secret? You can retrieve these in the Redpanda Cloud UI user panel.")
	for _, prompt := range []struct {
		name  string
		input survey.Prompt
		dst   *string
	}{
		{name: "Client ID", input: &survey.Input{Message: "Client ID:"}, dst: &clientID},
		{name: "Client secret", input: &survey.Password{Message: "Client Secret:"}, dst: &clientSecret},
	} {
		if err := survey.AskOne(prompt.input, prompt.dst, survey.WithValidator(survey.Required)); err != nil {
			return "", "", fmt.Errorf("failed to retrieve %s: %w", prompt.name, err)
		}
	}
	clientID = strings.TrimSpace(clientID)
	clientSecret = strings.TrimSpace(clientSecret)
	return clientID, clientSecret, nil
}
