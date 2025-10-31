// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package cluster

import (
	"errors"
	"fmt"
	"os"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/profile"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newSelectCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var profileName string
	var serverlessNetwork string

	cmd := &cobra.Command{
		Use:     "select [NAME]",
		Aliases: []string{"use"},
		Short:   "Update your rpk profile to talk to the requested cluster",
		Long: `Update your rpk profile to talk to the requested cluster.

This command is essentially an alias for the following command:

    rpk profile create --from-cloud=${NAME}

If you want to name this profile rather than creating or updating values in
the default cloud-dedicated profile, you can use the --profile flag.

For serverless clusters that support both public and private networking, you
will be prompted to select a network type unless you specify --serverless-network.
To avoid prompts in automation, explicitly set --serverless-network to 'public'
or 'private'.
`,
		Args: cobra.MaximumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)

			yAct, err := cfg.ActualRpkYamlOrEmpty()
			out.MaybeDie(err, "unable to load rpk.yaml: %v", err)
			yAuthVir := cfg.VirtualRpkYaml().CurrentAuth()

			name := "prompt"
			if len(args) == 1 {
				name = args[0]
			}

			err = profile.CreateFlow(cmd.Context(), fs, cfg, yAct, yAuthVir, "", "", name, false, nil, profileName, "", serverlessNetwork)
			if ee := (*profile.ProfileExistsError)(nil); errors.As(err, &ee) {
				fmt.Printf(`Unable to automatically create profile %q due to a name conflict with
an existing self-hosted profile, please rename that profile or use the
--profile flag to explicitly name your new profile.

Either:
    rpk profile select %[1]q
    rpk profile rename-to $something_else
    rpk cloud cluster select [NAME]
Or:
    rpk cloud cluster select [NAME] --profile $another_something
`, ee.Name)
				os.Exit(1)
			}
			out.MaybeDieErr(err)
		},
	}

	cmd.Flags().StringVar(&profileName, "profile", "", fmt.Sprintf("Name of a profile to create or update (avoids updating %q)", profile.RpkCloudProfileName))
	cmd.Flags().StringVar(&serverlessNetwork, "serverless-network", "", "Networking type for serverless clusters: 'public' or 'private' (if not specified, will prompt if both are available)")
	cmd.RegisterFlagCompletionFunc("serverless-network", func(_ *cobra.Command, _ []string, _ string) ([]string, cobra.ShellCompDirective) {
		return []string{"public", "private"}, cobra.ShellCompDirectiveNoFileComp
	})
	return cmd
}
