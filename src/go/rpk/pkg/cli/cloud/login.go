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
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/profile"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cloudapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/httpapi"
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

PROFILE SELECTION

This command by default attempts to populate a new profile that talks to a
cloud cluster for you. If you have an existing cloud profile, this will select
it, prompting which to use if you have many. If you have no cloud profile, this
command will prompt you to select one that exists in your organization. If you
want to disable automatic profile creation and selection, use --no-profile.
`,
		Run: func(cmd *cobra.Command, _ []string) {
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)
			y, err := cfg.ActualRpkYamlOrEmpty()
			out.MaybeDie(err, "unable to load config: %v", err)
			auth := y.Auth(y.CurrentCloudAuth)
			var cc bool
			if auth != nil {
				cc = auth.HasClientCredentials()
			}
			_, err = oauth.LoadFlow(cmd.Context(), fs, cfg, auth0.NewClient(cfg.DevOverrides()), noBrowser)
			if err != nil {
				fmt.Printf("Unable to login to Redpanda Cloud (%v).\n", err)
				if e := (*oauth.BadClientTokenError)(nil); errors.As(err, &e) && cc {
					fmt.Println(`You may need to clear your client ID and secret with 'rpk cloud logout --clear-credentials',
and then re-specify the client credentials next time you log in.`)
				}
				os.Exit(1)
			}

			if cc && save {
				yAct, _ := cfg.ActualRpkYaml() // must exist due to LoadFlow checking
				yAct.Auth(yAct.CurrentCloudAuth).ClientSecret = auth.ClientSecret
				err = yAct.Write(fs)
				out.MaybeDie(err, "unable to save client ID and client secret: %v", err)
			}
			fmt.Println("Successfully logged in.")
			if noProfile {
				return
			}

			msg, err := loginProfileFlow(cmd.Context(), fs, cfg, cfg.DevOverrides().CloudAPIURL)
			if err != nil {
				fmt.Printf("Unable to create and switch to profile: %v\n", err)
				fmt.Printf("Once any error is fixed, you can create a profile with\n")
				fmt.Printf("    rpk profile create {name} --from-cloud {cluster_id}\n")
				return
			}
			fmt.Print(msg)
		},
	}

	p.InstallCloudFlags(cmd)
	cmd.Flags().BoolVar(&noProfile, "no-profile", false, "Skip automatic profile creation and any associated prompts")
	cmd.Flags().BoolVar(&noBrowser, "no-browser", false, "Opt out of auto-opening authentication URL")
	cmd.Flags().BoolVar(&save, "save", false, "Save environment or flag specified client ID and client secret to the configuration file")
	return cmd
}

func loginProfileFlow(ctx context.Context, fs afero.Fs, cfg *config.Config, overrideCloudURL string) (string, error) {
	y, err := cfg.ActualRpkYamlOrEmpty()
	if err != nil {
		return "", fmt.Errorf("unable to load config: %v", err)
	}
	auth := y.Auth(y.CurrentCloudAuth)
	// If our current profile is a cloud cluster, we exit.
	// If one cloud profile exists, we switch to it.
	// If two+ cloud profiles exist, we prompt the user to select one.
	if p := y.Profile(y.CurrentProfile); p != nil && p.FromCloud {
		return "", nil
	}
	var cloudProfiles []*config.RpkProfile
	for i := range y.Profiles {
		p := &y.Profiles[i]
		if p.FromCloud {
			cloudProfiles = append(cloudProfiles, p)
		}
	}
	if len(cloudProfiles) == 1 {
		p := cloudProfiles[0]
		y.MoveProfileToFront(p)
		y.CurrentProfile = p.Name
		return fmt.Sprintf("Set current profile to %q.", p.Name), y.Write(fs)
	} else if len(cloudProfiles) > 1 {
		var names []string
		for _, p := range cloudProfiles {
			names = append(names, p.Name)
		}
		sort.Strings(names)
		name, err := out.Pick(names, "Which cloud profile would you like to switch to?")
		if err != nil {
			return "", err
		}
		for _, p := range cloudProfiles {
			if p.Name == name {
				y.MoveProfileToFront(p)
				y.CurrentProfile = p.Name
				return fmt.Sprintf("Set current profile to %q.", p.Name), y.Write(fs)
			}
		}
		panic("unreachable")
	}

	// Zero cloud profiles exist. We automatically create one from any
	// existing cloud cluster.
	// * One cluster exists: create profile for it and swap, no prompt.
	// * Multiple clusters exist: prompt which to choose and swap.
	cl := cloudapi.NewClient(overrideCloudURL, auth.AuthToken, httpapi.ReqTimeout(10*time.Second))

	pres, err := profile.PromptCloudClusterProfile(ctx, cl)
	if err != nil {
		if errors.Is(err, profile.ErrNoCloudClusters) {
			return `You currently have no cloud clusters, when you create one you can run
'rpk profile create --from-cloud' to create a profile for it.`, nil
		}
		return "", err
	}

	// Before pushing this profile, we first check if the name exists. If
	// so, we prompt.
	p := pres.Profile
	name := p.Name
	for {
		if y.Profile(name) == nil {
			break
		}
		p.Name, err = out.Prompt("Profile %q already exists, what would you like to name this new profile?", name)
		if err != nil {
			return "", err
		}
	}
	y.CurrentProfile = y.PushProfile(p)

	// We always print the cloud cluster message first, and then optionally
	// print a few extra things. Serverless never has MTLS nor SASL
	// messages, we don't need to worry about ordering much.
	msg := fmt.Sprintf("Created profile %q and set it as the current profile.\n", p.Name)
	msg += profile.CloudClusterMessage(p, pres.ClusterName, pres.ClusterID)
	if pres.MessageMTLS {
		msg += profile.RequiresMTLSMessage()
	}
	if pres.MessageSASL {
		msg += profile.RequiresSASLMessage()
	}
	if pres.IsServerlessHello {
		msg += profile.ServerlessHelloMessage()
	}
	return msg, y.Write(fs)
}
