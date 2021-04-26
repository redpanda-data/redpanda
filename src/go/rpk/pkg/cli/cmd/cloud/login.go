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
)

func NewLoginCommand(fs afero.Fs) *cobra.Command {
	return &cobra.Command{
		Use:   "login",
		Short: "Login to vectorized cloud",
		Long:  `Authorize rpk to access vectorized cloud with your user credentials.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			auth0Client := vcloud.NewDefaultAuth0Client()
			token, err := vcloud.Login(auth0Client)
			if err != nil {
				return fmt.Errorf("login failed, please retry.\n%w", err)
			}
			log.Info("Success!")
			log.Info(token.Token)
			return nil
		},
	}
}
