// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package connect

import (
	"fmt"
	"slices"
	"strings"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cobraext"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/plugin"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

func init() {
	plugin.RegisterManaged("connect", []string{"connect"}, func(cmd *cobra.Command, _ afero.Fs, p *config.Params) *cobra.Command {
		run := cmd.Run
		cmd.Run = func(cmd *cobra.Command, args []string) {
			pluginArgs, err := parseConnectFlags(p, cmd, args)
			out.MaybeDie(err, "unable to parse flags: %v", err)
			run(cmd, pluginArgs)
		}
		return cmd
	})
}

func NewCommand(fs afero.Fs, p *config.Params, execFn func(string, []string) error) *cobra.Command {
	cmd := &cobra.Command{
		Use:                "connect",
		Short:              "A stream processor for mundane tasks - https://docs.redpanda.com/redpanda-connect",
		DisableFlagParsing: true,                  // Required for managed plugins, we manually parse the flags.
		Args:               cobra.MinimumNArgs(0), // Connect can be run without commands.
		Run: func(cmd *cobra.Command, args []string) {
			pluginArgs, err := parseConnectFlags(p, cmd, args)
			out.MaybeDie(err, "unable to parse flags: %v", err)
			connect, pluginExists := plugin.ListPlugins(fs, plugin.UserPaths()).Find("connect")
			var pluginPath string
			if !pluginExists {
				// If it doesn't exist we only download when the user runs a
				// subcommand.
				var isSubcommand bool
				for _, arg := range pluginArgs {
					switch {
					case arg == "-c":
						out.Die("-c flag is not supported by this command; run 'rpk connect run' instead")
					case arg == "--version":
						fmt.Println("cannot get connect version: rpk connect is not installed; run 'rpk connect install'")
						cmd.Help()
						return
					case strings.HasPrefix(arg, "--") || strings.HasPrefix(arg, "-"):
						continue
					default:
						isSubcommand = true
					}
				}
				if !isSubcommand {
					cmd.Help()
					return
				}
				fmt.Println("Downloading latest Redpanda Connect")
				path, _, err := installConnect(cmd.Context(), fs, "latest")
				out.MaybeDie(err, "unable to install Redpanda Connect: %v; if running on an air-gapped environment you may install 'redpanda-connect' with your package manager.", err)
				pluginPath = path
			}
			if pluginExists {
				pluginPath = connect.Path
				if !connect.Managed {
					zap.L().Sugar().Warn("rpk is using a self-managed version of Redpanda Connect. If you want rpk to manage connect, use rpk connect uninstall && rpk connect install. To continue managing Connect manually, use our redpanda-connect package.")
				}
			}
			zap.L().Debug("executing connect plugin", zap.String("path", pluginPath), zap.Strings("args", pluginArgs))
			err = execFn(pluginPath, pluginArgs)
			out.MaybeDie(err, "unable to execute redpanda connect plugin: %v", err)
		},
	}
	cmd.AddCommand(
		installCommand(fs),
		uninstallCommand(fs),
		upgradeCommand(fs),
	)
	return cmd
}

func parseConnectFlags(p *config.Params, cmd *cobra.Command, args []string) ([]string, error) {
	f := cmd.Flags()

	keepForPlugin, stripForRpk := cobraext.StripFlagset(args, f)
	if err := f.Parse(stripForRpk); err != nil {
		return nil, err
	}
	// Since we are manually parsing the flags, we need to force build the
	// logger again.
	zap.ReplaceGlobals(p.BuildLogger())
	// We need to add back the Help and Version flags manually since we strip
	// them for rpk.
	if cobraext.LongFlagValue(args, f, "help", "h") == "true" && !slices.Contains(keepForPlugin, "--help") {
		keepForPlugin = append(keepForPlugin, "--help")
	}
	// In rpk --verbose has a shorthand -v, in connect -v is used for version.
	// This is _only_ valid for the 'connect' command:
	isSubCommand := slices.ContainsFunc(keepForPlugin, func(s string) bool { return !strings.HasPrefix(s, "-") })
	if cmd.Name() == "connect" && !isSubCommand {
		if cobraext.LongFlagValue(args, f, "verbose", "v") == "true" && !slices.Contains(keepForPlugin, "--version") && !slices.Contains(keepForPlugin, "-v") {
			keepForPlugin = append(keepForPlugin, "--version")
		}
	}
	return keepForPlugin, nil
}
