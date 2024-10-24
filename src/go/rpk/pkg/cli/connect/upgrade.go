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
	"context"
	"fmt"
	"os/exec"
	"strings"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/redpanda"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/plugin"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func upgradeCommand(fs afero.Fs) *cobra.Command {
	var noConfirm bool
	cmd := &cobra.Command{
		Use:   "upgrade",
		Short: "Upgrade to the latest Redpanda Connect version",
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, _ []string) {
			pluginDir, err := plugin.DefaultBinPath()
			out.MaybeDie(err, "unable to determine managed plugin path: %w", err)
			connect, pluginExists := plugin.ListPlugins(fs, plugin.UserPaths()).Find("connect")
			if !pluginExists {
				out.Die("Redpanda Connect plugin not found. You may install it running 'rpk connect install'")
			}
			// A user may have installed version of 'rpk connect' either
			// manually or using a package manager (air-gapped). An attempt to
			// upgrade can result in having multiple copies of connect. Instead,
			// we kindly ask to re-install.
			if !connect.Managed {
				out.Die("Found a self-managed Redpanda Connect plugin; unfortunately, we cannot upgrade it with this installation. Run rpk connect uninstall && rpk connect install, or to continue managing Connect manually, use our redpanda-connect package.")
			}
			art, version, err := getConnectArtifact(cmd.Context(), "latest")
			out.MaybeDieErr(err)

			currentSha, err := plugin.Sha256Path(fs, connect.Path)
			out.MaybeDie(err, "unable to determine the sha256sum of current Redpanda Connect %q: %v", connect.Path, err)

			if strings.HasPrefix(currentSha, art.Sha256) {
				out.Exit("Redpanda Connect already up-to-date")
			}
			currentVersion, err := connectVersion(cmd.Context(), connect.Path)
			out.MaybeDie(err, "unable to determine current version of Redpanda Connect: %v", err)

			if !noConfirm {
				latestVersion, err := redpanda.VersionFromString(version)
				out.MaybeDie(err, "unable to parse latest version of Redpanda Connect: %v", err)
				if latestVersion.Major > currentVersion.Major {
					confirmed, err := out.Confirm("Confirm major version upgrade from %v to %v?", currentVersion.String(), latestVersion.String())
					out.MaybeDie(err, "unable to confirm upgrade: %v", err)
					if !confirmed {
						out.Exit("Upgrade canceled.")
					}
				}
			}

			_, err = downloadAndInstallConnect(cmd.Context(), fs, pluginDir, art.Path, art.Sha256)
			out.MaybeDieErr(err)

			fmt.Printf("Redpanda Connect successfully upgraded from %v to the latest version (%v).\n", currentVersion.String(), version)
		},
	}
	cmd.Flags().BoolVar(&noConfirm, "no-confirm", false, "Disable confirmation prompt for major version upgrades")
	return cmd
}

// connectVersion executes rpk connect --version and parses the current version
// from the output.
func connectVersion(ctx context.Context, connectPath string) (redpanda.Version, error) {
	versionCmd := exec.CommandContext(ctx, connectPath, "--version")
	var sb strings.Builder
	versionCmd.Stdout = &sb
	if err := versionCmd.Run(); err != nil {
		return redpanda.Version{}, err
	}
	// Command output is:
	//   Version: <Version>
	//   Date: <Build date>
	versionPrefix := "Version: "
	var versionStr string
	for _, l := range strings.Split(sb.String(), "\n") {
		if strings.HasPrefix(l, versionPrefix) {
			versionStr = strings.TrimPrefix(l, versionPrefix)
			break
		}
	}
	version, err := redpanda.VersionFromString(strings.TrimSpace(versionStr))
	if err != nil {
		return redpanda.Version{}, fmt.Errorf("unable to determine Redpanda version from %q: %v", versionStr, err)
	}
	return version, nil
}
