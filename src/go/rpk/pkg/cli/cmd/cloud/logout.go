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
	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/vcloud/config"
)

func NewLogoutCommand(fs afero.Fs) *cobra.Command {
	return &cobra.Command{
		Use:   "logout",
		Short: "Logout from vectorized cloud",
		Long:  `If there's a token stored locally after you've logged in, it will be wiped out.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			configRW := config.NewVCloudConfigReaderWriter(fs)
			// write an empty string to the config
			err := configRW.WriteToken("")
			if err != nil {
				log.Info("Error while logging out:")
				return err
			}
			log.Info("Logged out!")
			return nil
		},
	}
}
