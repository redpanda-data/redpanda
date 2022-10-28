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

func newLoginCommand() *cobra.Command {
	var params auth.Params
	cmd := &cobra.Command{
		Use:   "login",
		Short: "Login to the Redpanda cloud",
		Long: `Login to the Redpanda cloud

This command checks for an existing token and, if present, ensures it is still
valid. If no token is found or the token is no longer valid, this command will
login and save your token.

You may use any of the following methods to pass the cloud credentials to rpk:

CONFIG FILE

Store them in '$HOME/.config/rpk/__cloud.yaml' :
  client_id:
  client_secret

FLAGS

Use --client-id and --client-secret flags:

  rpk cloud login --client-id <id> --client-secret <secret>

ENVIRONMENT VARIABLES

Use CLOUD_CLIENT_ID and CLOUD_CLIENT_SECRET environment flags

  CLOUD_CLIENT_ID=<id> CLOUD_CLIENT_SECRET=<secret> rpk cloud login

If neither of the above methods are used, rpk prompts for the credentials and
store them in $HOME/.config/rpk/__cloud.yaml once the login succeeds.

All commands in the cloud plugin check for / load / refresh a token as
necessary, so this command is not really necessary to run directly.
`,
		Run: func(cmd *cobra.Command, _ []string) {
			err := auth.LoadFlow(cmd.Context(), params)
			out.MaybeDie(err, "unable to login into Redpanda cloud: %v", err)
			fmt.Println("Successfully logged in")
		},
	}

	cmd.Flags().StringVar(&params.ClientID, auth.FlagClientID, "", "The client ID of the organization in Redpanda Cloud")
	cmd.Flags().StringVar(&params.ClientSecret, auth.FlagClientSecret, "", "The client secret of the organization in Redpanda Cloud")
	cmd.MarkFlagsRequiredTogether(auth.FlagClientID, auth.FlagClientSecret)
	return cmd
}
