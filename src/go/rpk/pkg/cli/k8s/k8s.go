// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// Package k8s wires the Redpanda Kubernetes CLI into rpk as a managed plugin.
// Users interact with it as `rpk k8s ...`; on-disk the binary is the standard
// rpk managed-plugin layout under ~/.local/bin.
package k8s

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
	// When a `rpk k8s <subcommand>` managed-plugin leaf is dispatched, strip
	// rpk global flags before the child process is exec'd. The k8s plugin
	// reads kubeconfig itself, so no env/cloud injection is required.
	plugin.RegisterManaged(pluginSlug, []string{"k8s"}, func(cmd *cobra.Command, _ afero.Fs, p *config.Params) *cobra.Command {
		run := cmd.Run
		cmd.Run = func(cmd *cobra.Command, args []string) {
			pluginArgs, err := parseFlags(p, cmd, args)
			out.MaybeDie(err, "unable to prepare rpk k8s invocation: %v", err)
			run(cmd, pluginArgs)
		}
		return cmd
	})
}

// NewCommand returns the top-level `rpk k8s` cobra command. If the plugin is
// installed, `rpk k8s <sub>` hands off to it; otherwise we auto-install on
// first subcommand use, matching the rpk ai/connect pattern.
func NewCommand(fs afero.Fs, p *config.Params, execFn func(string, []string) error) *cobra.Command {
	cmd := &cobra.Command{
		Use:                "k8s",
		Short:              "Interact with Redpanda clusters running on Kubernetes",
		DisableFlagParsing: true,
		Args:               cobra.MinimumNArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			pluginArgs, err := parseFlags(p, cmd, args)
			out.MaybeDie(err, "unable to prepare rpk k8s invocation: %v", err)

			k, pluginExists := plugin.ListPlugins(fs, plugin.UserPaths()).Find(pluginSlug)

			var isSubcommand, isVersion bool
			for _, arg := range pluginArgs {
				switch {
				case arg == "--version":
					isVersion = true
				case strings.HasPrefix(arg, "--") || strings.HasPrefix(arg, "-"):
					continue
				default:
					isSubcommand = true
				}
			}
			if !pluginExists && isVersion {
				fmt.Println("cannot get version: the rpk k8s plugin is not installed; run 'rpk k8s install'")
				return
			}
			if !isSubcommand && !isVersion {
				cmd.Help()
				return
			}

			var pluginPath string
			if !pluginExists {
				maybeExitFIPS()
				fmt.Fprintln(os.Stderr, "Downloading latest Redpanda Kubernetes plugin")
				path, _, err := installK8sPlugin(cmd.Context(), fs, "latest")
				out.MaybeDie(err, "unable to install the rpk k8s plugin: %v; if running in an air-gapped environment you may install it manually", err)
				pluginPath = path
			} else {
				pluginPath = k.Path
				if !k.Managed {
					zap.L().Sugar().Warn("rpk is using a self-managed version of the rpk k8s plugin. If you want rpk to manage it, run 'rpk k8s uninstall && rpk k8s install'. To continue managing it manually, keep using your existing install.")
				}
			}
			zap.L().Debug("executing rpk k8s plugin", zap.String("path", pluginPath), zap.Strings("args", pluginArgs))
			err = execFn(pluginPath, pluginArgs)
			out.MaybeDie(err, "unable to execute the rpk k8s plugin: %v", err)
		},
	}
	cmd.AddCommand(
		installCommand(fs),
		uninstallCommand(fs),
		upgradeCommand(fs),
	)
	return cmd
}
