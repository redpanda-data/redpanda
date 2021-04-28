// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package cloud

import (
	"fmt"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/vcloud"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/vcloud/config"
)

func NewLoginCommand(fs afero.Fs) *cobra.Command {
	return &cobra.Command{
		Use:   "login",
		Short: "Login to vectorized cloud",
		Long:  `Authorize rpk to access vectorized cloud with your user credentials.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			// try to look for existing token in config first
			configRW := config.NewVCloudConfigReaderWriter(fs)
			token, err := configRW.ReadToken()
			if err != nil && err != config.ErrConfigFileDoesNotExist {
				return fmt.Errorf("error reading token from config file: %w", err)
			}
			auth0Client := vcloud.NewDefaultAuth0Client()
			if err == config.ErrConfigFileDoesNotExist {
				// no config file, have to log in
				return login(auth0Client, configRW)
			}

			// validate token from config
			valid, err := auth0Client.VerifyToken(token)
			if err != nil {
				return fmt.Errorf("error when validating token from config. %w", err)
			}
			if valid {
				log.Info("Already logged in!")
				return nil
			}
			// token not valid, need to log in
			return login(auth0Client, configRW)
		},
	}
}

func login(
	authClient vcloud.AuthClient, configRW config.ConfigReaderWriter,
) error {
	// token not valid or first time logging in
	newToken, err := vcloud.Login(authClient)
	if err != nil {
		return fmt.Errorf("login failed, please retry.\n%w", err)
	}

	// write token to config
	err = configRW.WriteToken(newToken.Token)
	if err != nil {
		return fmt.Errorf("error writing token into the config file. %w", err)
	}
	log.Info("Success!")
	return nil
}
