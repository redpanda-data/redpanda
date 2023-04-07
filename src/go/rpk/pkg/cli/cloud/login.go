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
	"errors"
	"fmt"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cloud/cloudcfg"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/oauth"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/oauth/providers/auth0"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newLoginCommand(fs afero.Fs) *cobra.Command {
	var params cloudcfg.Params
	var save bool
	cmd := &cobra.Command{
		Use:   "login",
		Short: "Log in to the Redpanda cloud",
		Args:  cobra.ExactArgs(0),
		Long: `Log in to the Redpanda cloud

This command checks for an existing Redpanda Cloud API token and, if present, 
ensures it is still valid. If no token is found or the token is no longer valid, 
this command will login and save your token along with the client ID used to 
request the token.

You may use any of the following methods to pass the cloud credentials to rpk:

SSO:

This will automatically launch your default web browser and prompt you to 
authenticate via our Redpanda Cloud page. Once you have successfully 
authenticated, you will be ready to use rpk cloud commands.

CLIENT CREDENTIALS:

Cloud client credentials can be used to login to Redpanda, they can be created 
in the Clients tab of the Users section in the Redpanda Cloud online interface. 
client credentials can be provided in three ways, in order of preference:

* In $HOME/.config/rpk/__cloud.yaml, in 'client_id' and 'client_secret' fields
* Through RPK_CLOUD_CLIENT_ID and RPK_CLOUD_CLIENT_SECRET environment variables
* Through the --client-id and --client-secret flags

If none of these are provided, rpk will use the SSO method to login. 
If you specify environment variables or flags, they will not be synced to the
__cloud.yaml file unless the --save flag is passed. The cloud authorization 
token and client ID is always synced.
`,
		Run: func(cmd *cobra.Command, _ []string) {
			cfg, err := params.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			_, err = oauth.LoadFlow(cmd.Context(), fs, cfg, auth0.NewClient(cfg))
			if e := (*oauth.BadClientTokenError)(nil); errors.As(err, &e) {
				out.Die(`
Unable to login into Redpanda Cloud: %v.

You may need to clear your client ID and secret with 'rpk cloud logout --clear-credentials',
and then re-specify the client credentials next time you log in.`, err)
			}
			out.MaybeDie(err, "unable to login into Redpanda Cloud: %v", err)
			if save {
				err = cfg.SaveAll(fs)
				out.MaybeDie(err, "unable to save client ID and client secret: %v", err)
			}
			fmt.Println("Successfully logged in.")
		},
	}

	cmd.Flags().BoolVar(&save, "save", false, "Save environment or flag specified client ID and client secret to the configuration file")
	cmd.Flags().StringVar(&params.ClientID, cloudcfg.FlagClientID, "", "The client ID of the organization in Redpanda Cloud")
	cmd.Flags().StringVar(&params.ClientSecret, cloudcfg.FlagClientSecret, "", "The client secret of the organization in Redpanda Cloud")
	cmd.MarkFlagsRequiredTogether(cloudcfg.FlagClientID, cloudcfg.FlagClientSecret)
	return cmd
}
