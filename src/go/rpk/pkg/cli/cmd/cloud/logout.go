// Copyright 2022 Redpanda Data, Inc.
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

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cmd/cloud/auth"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/cobra"
)

func newLogoutCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "logout",
		Short: "Log out from the Redpanda cloud",
		Run: func(cmd *cobra.Command, args []string) {
			cfg, err := auth.LoadConfig()
			out.MaybeDie(err, "unable to load the cloud configuration: %v", err)

			if cfg == nil || cfg.AuthToken == "" {
				fmt.Println("You are not logged in")
				return
			}

			cfg.AuthToken = ""
			err = cfg.Save()
			out.MaybeDie(err, "unable to save the cloud configuration :%v", err)
			fmt.Println("You are now logged out")
		},
	}

	return cmd
}
