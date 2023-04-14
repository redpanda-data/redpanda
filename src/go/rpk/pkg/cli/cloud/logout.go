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

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cloud/cloudcfg"
	rpkos "github.com/redpanda-data/redpanda/src/go/rpk/pkg/os"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newLogoutCommand(fs afero.Fs) *cobra.Command {
	var (
		params cloudcfg.Params
		clear  bool
	)
	cmd := &cobra.Command{
		Use:   "logout",
		Short: "Log out from Redpanda cloud",
		Long: `Log out from Redpanda cloud

This command deletes your cloud auth token. If you want to log out entirely and
switch to a different organization, you can use the --clear-credentials flag to
additionally clear your client ID and client secret.
`,
		Args: cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			if rpkos.IsRunningSudo() {
				out.Die("detected rpk is running with sudo; please execute this command without sudo to avoid saving the cloud configuration as a root owned file")
			}
			cfg, err := params.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			if cfg.AuthToken == "" && !clear {
				fmt.Println("You are not logged in.")
				return
			}
			if clear {
				cfg.ClearCredentials()
				err = cfg.SaveAll(fs)
			} else {
				cfg.AuthToken = ""
				err = cfg.SaveToken(fs)
			}
			out.MaybeDie(err, "unable to save the cloud configuration :%v", err)
			fmt.Println("You are now logged out.")
		},
	}

	cmd.Flags().BoolVarP(&clear, "clear-credentials", "c", false, "Clear the client ID and client secret in addition to the auth token")
	return cmd
}
