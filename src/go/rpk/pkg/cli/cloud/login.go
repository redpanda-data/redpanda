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
	"os"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/container/common"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/profile"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/oauth"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/oauth/providers/auth0"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newLoginCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var save, noProfile, noBrowser bool
	cmd := &cobra.Command{
		Use:   "login",
		Short: "Log in to the Redpanda cloud",
		Args:  cobra.ExactArgs(0),
		Long: `Log in to the Redpanda cloud

This command checks for an existing Redpanda Cloud API token and, if present, 
ensures it is still valid. If no token is found or the token is no longer valid, 
this command will login and save your token along with the client ID used to 
request the token.

You may use either SSO or client credentials to log in.

SSO

This will automatically launch your default web browser and prompt you to 
authenticate via our Redpanda Cloud page. Once you have successfully 
authenticated, you will be ready to use rpk cloud commands.

You may opt out of auto-opening the browser by passing the '--no-browser' flag.

CLIENT CREDENTIALS

Cloud client credentials can be used to login to Redpanda, they can be created 
in the Clients tab of the Users section in the Redpanda Cloud online interface. 
client credentials can be provided in three ways, in order of preference:

* In your rpk cloud auth, 'client_id' and 'client_secret' fields
* Through RPK_CLOUD_CLIENT_ID and RPK_CLOUD_CLIENT_SECRET environment variables
* Through the --client-id and --client-secret flags

If none of these are provided, rpk will use the SSO method to login. 
If you specify environment variables or flags, they will not be synced to the
rpk.yaml file unless the --save flag is passed. The cloud authorization 
token and client ID is always synced.
`,
		Run: func(cmd *cobra.Command, _ []string) {
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			yAct, err := cfg.ActualRpkYamlOrEmpty()
			out.MaybeDie(err, "rpk unable to load config: %v", err)

			p := yAct.Profile(yAct.CurrentProfile)
			authAct, authVir, clearedProfile, _, err := oauth.LoadFlow(cmd.Context(), fs, cfg, auth0.NewClient(cfg.DevOverrides()), noBrowser, true, cfg.DevOverrides().CloudAPIURL)
			if err != nil {
				fmt.Printf("Unable to login to Redpanda Cloud (%v).\n", err)
				if e := (*oauth.BadClientTokenError)(nil); errors.As(err, &e) && authVir != nil && authVir.HasClientCredentials() {
					fmt.Println(`You may need to clear your client ID and secret with 'rpk cloud logout --clear-credentials',
and then re-specify the client credentials next time you log in.`)
				} else {
					fmt.Println(`You may need to clear your credentials with 'rpk cloud logout --clear-credentials', and login again`)
				}
				os.Exit(1)
			}
			if authVir.HasClientCredentials() && save {
				authAct.ClientSecret = authVir.ClientSecret
				err = yAct.Write(fs)
				out.MaybeDie(err, "unable to save client ID and client secret: %v", err)
			}

			fmt.Printf("Successfully logged into cloud organization %q (%s) via %s.\n", authAct.Organization, authAct.OrgID, authAct.Kind)
			fmt.Println()

			// No profile, or you have profiles but none were selected.
			// * If --no-profile, print a message and exit
			// * Otherwise, prompt for cloud cluster selection
			if p == nil || len(yAct.Profiles) == 0 {
				if noProfile {
					fmt.Println(`To create an rpk profile to talk to an existing cloud cluster, use 'rpk cloud cluster select'.
To learn more about profiles, check 'rpk profile --help'.
You are not currently in a profile; rpk talks to a localhost:9092 cluster by default.`)
				} else {
					fmt.Println("rpk will switch to a cloud cluster profile automatically, if you want to interrupt this process, you can ctrl+c now.")
					err = profile.CreateFlow(cmd.Context(), fs, cfg, yAct, authVir, "", "", "prompt", false, nil, "", "")
					profile.MaybeDieExistingName(err)
				}
				return
			}

			// User had a profile, but it was cleared due to the browser having
			// a different org's auth.
			// * If --no-profile, print a message and exit
			// * Otherwise, prompt for cloud cluster selection
			if p != nil && clearedProfile {
				priorAuth := p.ActualAuth()
				fmt.Printf("rpk swapped away from your prior profile %q which was authenticated with organization %q (%s).\n", p.Name, priorAuth.Organization, priorAuth.OrgID)
				if noProfile {
					fmt.Printf(`
To create a new rpk profile for a cluster in this organization, try either:
    rpk profile create --from-cloud
    rpk cloud cluster select

rpk will talk to a localhost:9092 cluster until you swap to a different profile.
`)
				} else {
					fmt.Println("rpk will switch to a cloud cluster profile automatically, if you want to interrupt this process, you can ctrl+c now.")
					err = profile.CreateFlow(cmd.Context(), fs, cfg, yAct, authVir, "", "", "prompt", false, nil, "", "")
					profile.MaybeDieExistingName(err)
				}
				return
			}

			// Below here, the current profile is pointed to a
			// local container cluster or a self hosted cluster.
			// We want to create or swap to the cloud profile,
			// unless the user used --no-profile.
			if noProfile {
				// The current profile is seemingly pointing to a container cluster.
				if p.Name == common.ContainerProfileName {
					fmt.Printf("You are talking to a localhost 'rpk container' cluster (rpk profile name: %q)", p.Name)
					fmt.Println("To talk to a cloud cluster, use 'rpk cloud cluster select'.")
					return
				}
				// The current profile is a self hosted cluster.
				fmt.Printf("You are talking to a self hosted cluster (rpk profile name: %q)\n", p.Name)
				fmt.Println("To talk to a cloud cluster, use 'rpk cloud cluster select'.")
				return
			}

			// The current profile was auth'd to the current organization.
			// We tell the status of what org the user is talking to.
			if p.FromCloud {
				fmt.Printf("You are talking to a cloud cluster %q (rpk profile name: %q)\n", p.CloudCluster.FullName(), p.Name)
				fmt.Println("Select a different cluster to talk to (or ctrl+c to keep the current cluster)?")
				err = profile.CreateFlow(cmd.Context(), fs, cfg, yAct, authVir, "", "", "prompt", false, nil, "", "")
				profile.MaybeDieExistingName(err)
				return
			}

			if p.Name == common.ContainerProfileName {
				fmt.Printf("You are talking to a localhost 'rpk container' cluster (rpk profile name: %q)\n", p.Name)
			} else {
				fmt.Printf("You are talking to a self hosted cluster (rpk profile name: %q)\n", p.Name)
			}
			fmt.Println("rpk will switch to a cloud cluster profile automatically, if you want to interrupt this process and keep your current profile, you can ctrl+c now.")

			// Prompt and switch.
			err = profile.CreateFlow(cmd.Context(), fs, cfg, yAct, authVir, "", "", "prompt", false, nil, "", "")
			profile.MaybeDieExistingName(err)
		},
	}

	p.InstallCloudFlags(cmd)
	cmd.Flags().BoolVar(&noBrowser, "no-browser", false, "Opt out of auto-opening authentication URL")
	cmd.Flags().BoolVar(&save, "save", false, "Save environment or flag specified client ID and client secret to the configuration file")
	cmd.Flags().BoolVar(&noProfile, "no-profile", false, "Skip automatic profile creation and any associated prompts")

	return cmd
}
