// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// Package ai wires the Redpanda AI CLI (rpai) into rpk as a managed plugin.
// Users interact with it as `rpk ai ...`; on-disk the binary lives at
// ~/.local/bin/.rpk.managed-rpai.
package ai

import (
	"fmt"
	"os"
	"strings"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/plugin"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

func init() {
	// Whenever a `rpk ai ...` managed-plugin invocation is dispatched, we
	// run our hook to inject RPAI_TOKEN / RPAI_ENDPOINT and strip rpk
	// global flags before the child process execs.
	plugin.RegisterManaged("rpai", []string{"ai"}, func(cmd *cobra.Command, fs afero.Fs, p *config.Params) *cobra.Command {
		run := cmd.Run
		cmd.Run = func(cmd *cobra.Command, args []string) {
			pluginArgs, err := applyHook(fs, p, cmd, args)
			out.MaybeDie(err, "unable to prepare rpk ai invocation: %v", err)
			run(cmd, pluginArgs)
		}
		return cmd
	})
}

// NewCommand returns the top-level `rpk ai` cobra command. If the rpai plugin
// is already installed, `rpk ai <sub>` hands off to it; otherwise we
// auto-install on first subcommand use, matching the rpk connect pattern.
func NewCommand(fs afero.Fs, p *config.Params, execFn func(string, []string) error) *cobra.Command {
	cmd := &cobra.Command{
		Use:                "ai",
		Short:              "Manage the Redpanda AI Gateway via rpai - https://docs.redpanda.com/redpanda-ai-gateway",
		DisableFlagParsing: true,                  // Required for managed plugins; we parse flags ourselves.
		Args:               cobra.MinimumNArgs(0), // Allow `rpk ai` with no subcommand (renders help).
		Run: func(cmd *cobra.Command, args []string) {
			pluginArgs, err := applyHook(fs, p, cmd, args)
			out.MaybeDie(err, "unable to prepare rpk ai invocation: %v", err)
			rpai, pluginExists := plugin.ListPlugins(fs, plugin.UserPaths()).Find("rpai")
			var pluginPath string
			if !pluginExists {
				// Without the plugin present, only download when the user
				// actually invoked a subcommand. Bare `rpk ai` or `rpk ai
				// --help` should just show help.
				var isSubcommand bool
				for _, arg := range pluginArgs {
					switch {
					case arg == "--version":
						fmt.Println("cannot get rpai version: the Redpanda AI CLI is not installed; run 'rpk ai install'")
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
				maybeExitFIPS()
				fmt.Fprintln(os.Stderr, "Downloading latest Redpanda AI CLI")
				path, _, err := installRpai(cmd.Context(), fs, "latest")
				out.MaybeDie(err, "unable to install the Redpanda AI CLI: %v; if running in an air-gapped environment you may install 'rpai' with your package manager", err)
				pluginPath = path
			}
			if pluginExists {
				pluginPath = rpai.Path
				if !rpai.Managed {
					zap.L().Sugar().Warn("rpk is using a self-managed version of the Redpanda AI CLI. If you want rpk to manage rpai, run 'rpk ai uninstall && rpk ai install'. To continue managing rpai manually, keep using your existing rpai install.")
				}
			}
			if cmd.Flags().Changed("help") {
				cmd.Help()
				return
			}
			zap.L().Debug("executing rpai plugin", zap.String("path", pluginPath), zap.Strings("args", pluginArgs))
			err = execFn(pluginPath, pluginArgs)
			out.MaybeDie(err, "unable to execute the Redpanda AI CLI plugin: %v", err)
		},
	}
	cmd.AddCommand(
		installCommand(fs),
		uninstallCommand(fs),
		upgradeCommand(fs),
	)
	return cmd
}
