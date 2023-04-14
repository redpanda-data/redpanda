// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package byoc

import (
	"errors"
	"strings"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cloud/cloudcfg"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cobraext"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/plugin"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func init() {
	// We manage the byoc plugin, and we install it under "rpk cloud byoc".
	// Whenever we run a byoc subcommand, we want to load our token and
	// pass it to the subcommand as an extra flag.
	plugin.RegisterManaged("byoc", []string{"cloud", "byoc"}, func(cmd *cobra.Command, fs afero.Fs) *cobra.Command {
		var params cloudcfg.Params
		run := cmd.Run

		// Plugin commands disable flag parsing because we want to pass
		// raw flags directly to the plugin. We are hijacking the exec
		// and want to parse a few flags ourselves.
		cmd.Run = func(cmd *cobra.Command, args []string) {
			var redpandaID string
			err := parseExecFlags(args, &params.ClientID, &params.ClientSecret, &redpandaID, true)
			out.MaybeDieErr(err)

			// We require our plugin to always be the exact version
			// pinned in the control plane.
			cfg, err := params.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)
			_, token, _, err := loginAndEnsurePluginVersion(cmd.Context(), fs, cfg, redpandaID)
			out.MaybeDie(err, "unable to ensure byoc plugin version: %v", err)

			// Finally, exec.
			run(cmd, append(args, "--cloud-api-token", token))
		}
		return cmd
	})
}

func findFlag(args []string, flag string) string {
	for i, arg := range args {
		// "--foo bar"
		if arg == flag && len(args) > i {
			return args[i+1]
		}
		// "--foo=bar"
		if strings.HasPrefix(arg, flag+"=") {
			return strings.TrimPrefix(arg, flag+"=")
		}
	}
	return ""
}

// For our byoc command, and for plugin shadow commands, we disable flag
// parsing so that we can pass raw flags to the plugin itself. However, we want
// to hijack a few of these flags within byoc.
//
// --client-id and --client-secret can override our auth flow, and
// --redpanda-id is potentially required.
func parseExecFlags(args []string, clientID, clientSecret, redpandaID *string, requireRedpandaID bool) error {
	flagClientID := findFlag(args, "--"+cloudcfg.FlagClientID)
	flagClientSecret := findFlag(args, "--"+cloudcfg.FlagClientSecret)
	if flagClientID != "" || flagClientSecret != "" {
		if flagClientID == "" || flagClientSecret == "" {
			return errors.New("--client-id and --client-secret are required together if either are used")
		}
		*clientID = flagClientID
		*clientSecret = flagClientSecret
	}

	// We require --redpanda-id when execing the plugin.
	flagRedpandaID := findFlag(args, "--redpanda-id")
	if flagRedpandaID == "" && requireRedpandaID {
		return errors.New("missing required --redpanda-id flag")
	}
	*redpandaID = flagRedpandaID

	// We disallow manually specifying --cloud-api-token,
	// which we load ourselves.
	if token := findFlag(args, "--cloud-api-token"); token != "" {
		return errors.New("--cloud-api-token cannot be manually specified")
	}
	return nil
}

// NewCommand returns a new byoc plugin command.
func NewCommand(fs afero.Fs, execFn func(string, []string) error) *cobra.Command {
	var params cloudcfg.Params
	cmd := &cobra.Command{
		Use:   "byoc",
		Short: "Manage a Redpanda cloud BYOC agent",
		Long: `Manage a Redpanda cloud BYOC agent

For BYOC, Redpanda installs an agent service in your owned cluster. The agent
then proceeds to provision further infrastructure and eventually, a full
Redpanda cluster.

The BYOC command runs Terraform to create and start the agent. You first need
a redpanda-id (or cluster ID); this is used to get the details of how your
agent should be provisioned. You can create a BYOC cluster in our cloud UI
and then come back to this command to complete the process.
`,
		DisableFlagParsing: true,
		Run: func(cmd *cobra.Command, args []string) {
			var redpandaID string
			err := parseExecFlags(args, &params.ClientID, &params.ClientSecret, &redpandaID, false)
			out.MaybeDieErr(err)

			// Now that we have parsed our required flags, we
			// strip every rpk specific flag from this command.
			// Every remaining flag is passed through to the
			// plugin.
			args = cobraext.StripFlagset(args, cmd.Flags())

			cfg, err := params.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			// We bind rpk to the plugin implementation a little
			// bit: we only want to download and exec the plugin if
			// it *looks* like the user is trying a direct plugin
			// command. Since we are disable flag parsing, this is
			// a little bit tricky: we have to find the first arg,
			// not --flag=val nor --flag val.
			//
			// This has edge cases that we are not handling, such
			// as a person using short flags, bool flags, etc.
			// We cannot handle these edge cases because at this
			// point, we do now know if the subcommand has default
			// values for flags or if it has bool flags, etc. So,
			// this is mostly best effort, but we do not expect
			// the plugin to be complicated.
			var isKnown bool
			for i := 0; i < len(args); i++ {
				arg := args[i]
				switch {
				case strings.HasPrefix(arg, "--") && !strings.Contains(arg, "="):
					i++
				case arg == "aws":
					isKnown = true
				case arg == "gcp":
					isKnown = true
				}
			}

			if !isKnown || redpandaID == "" {
				cmd.Help()
				return
			}

			path, token, _, err := loginAndEnsurePluginVersion(cmd.Context(), fs, cfg, redpandaID)
			out.MaybeDie(err, "unable to ensure byoc plugin version: %v", err)

			err = execFn(path, append(args, "--cloud-api-token", token))
			out.MaybeDie(err, "unable to execute plugin: %v", err)
		},
	}

	// We add these flags, but they will not be parsed because of
	// DisableFlagParsing. We manually parse them.
	cmd.Flags().StringVar(&params.ClientID, cloudcfg.FlagClientID, "", "The client ID of the organization in Redpanda Cloud")
	cmd.Flags().StringVar(&params.ClientSecret, cloudcfg.FlagClientSecret, "", "The client secret of the organization in Redpanda Cloud")
	cmd.MarkFlagsRequiredTogether(cloudcfg.FlagClientID, cloudcfg.FlagClientSecret)

	cmd.AddCommand(
		newInstallCommand(fs),
		newUninstallCommand(fs),
	)

	return cmd
}
