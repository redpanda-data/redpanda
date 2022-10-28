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
	"os"

	"github.com/AlecAivazis/survey/v2"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/auth0"
)

// The auth0 endpoint information to get dev tokens from.
var auth0Endpoint = auth0.Endpoint{
	URL:      "https://prod-cloudv2.us.auth0.com",
	Audience: "cloudv2-production.redpanda.cloud",
}

// LoadFlow loads or creates a config at default path, and validates and
// refreshes or creates an auth0 token using the given auth0 parameters.
//
// This function is expected to be called at the start of most commands.
func LoadFlow(ctx context.Context, params Params) error {
	var (
		id        string
		prompt    bool
		save      bool
		secret    string
		useParams bool

		cfg, err = LoadConfig()
	)

	switch {
	case err == nil: // loaded
		if cfg.ClientID == "" || cfg.ClientSecret == "" {
			fmt.Println("No client config found.")
			prompt = true
		}
		id = cfg.ClientID
		secret = cfg.ClientSecret
	case errors.Is(err, os.ErrNotExist): // missing file: prompt, auth and create
		fmt.Println("No configuration found.")
		cfg = &Config{}
		prompt = true
	default:
		return fmt.Errorf("unable to load your configuration: %v", err)
	}

	// We check if the flags or the environment variables were set, if that's
	// the case then we will use those values instead of the cfg file, but at
	// the end we will only save the token but not the Client ID/Secret.
	params.EnvOverride()
	if params.ClientID != "" || params.ClientSecret != "" {
		useParams = true
		id = params.ClientID
		secret = params.ClientSecret
	}

	if prompt && !useParams {
		err = promptClientCfg(cfg)
		if err != nil {
			return err
		}
		save = true
		id = cfg.ClientID
		secret = cfg.ClientSecret
	}

	defer func() {
		if save {
			err = cfg.Save()
			if err != nil {
				fmt.Printf("Unable to store your configuration: %v \nConfiguration: %v", err, cfg.Pretty())
			}
		}
	}()

	if cfg.AuthToken != "" {
		doRefresh, err := validateToken(cfg.AuthToken, id) //nolint:contextcheck // jwx/jwt package uses ctx.Background in a function down the stream
		if err != nil || !doRefresh {
			return err
		}
	}
	resp, err := auth0.NewClient(auth0Endpoint).GetToken(ctx, id, secret)
	if err != nil {
		save = false
		return fmt.Errorf("unable to retrieve the token: %v", err)
	}

	cfg.AuthToken = resp.AccessToken
	save = true
	return nil
}

// validateToken validates a token and returns whether a refresh is needed and
// notifies the user if it does.
func validateToken(token, clientID string) (doRefresh bool, err error) {
	err = auth0.ValidateToken(token, auth0Endpoint.Audience, clientID)
	if err == nil {
		return false, nil
	}
	if exp := new(auth0.ExpiredError); errors.As(err, &exp) {
		fmt.Println("Your token is expired, refreshing...")
		return true, nil
	}
	return true, nil
}

// promptClientCfg prompts users for the Client ID and Client secret and stores
// the answer in cfg.
func promptClientCfg(cfg *Config) error {
	fmt.Println("To retrieve your client configuration go to your organization's configuration page in the Redpanda cloud UI")
	qs := []*survey.Question{
		{
			Name:     "ClientID",
			Prompt:   &survey.Input{Message: "Client ID:"},
			Validate: survey.Required,
		},
		{
			Name:     "ClientSecret",
			Prompt:   &survey.Input{Message: "Client Secret:"},
			Validate: survey.Required,
		},
	}
	err := survey.Ask(qs, cfg)
	if err != nil {
		return err
	}
	return nil
}
