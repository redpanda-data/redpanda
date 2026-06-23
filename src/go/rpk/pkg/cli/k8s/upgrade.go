// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package k8s

import (
	"context"
	"fmt"
	"os/exec"
	"strings"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/plugin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/redpanda"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func upgradeCommand(fs afero.Fs) *cobra.Command {
	var noConfirm bool
	cmd := &cobra.Command{
		Use:   "upgrade",
		Short: "Upgrade to the latest Redpanda Kubernetes plugin version",
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, _ []string) {
			maybeExitFIPS()
			pluginDir, err := plugin.DefaultBinPath()
			out.MaybeDie(err, "unable to determine managed plugin path: %v", err)
			k, pluginExists := plugin.ListPlugins(fs, plugin.UserPaths()).Find(pluginSlug)
			if !pluginExists {
				out.Die("unable to find the Redpanda Kubernetes plugin. You may install it running 'rpk k8s install'")
			}
			if !k.Managed {
				out.Die("found a self-managed Redpanda Kubernetes plugin; unfortunately, we cannot upgrade it with this installation. Run 'rpk k8s uninstall && rpk k8s install', or to keep managing it manually, keep using the version you installed")
			}
			art, version, err := getK8sPluginArtifact(cmd.Context(), "latest")
			out.MaybeDieErr(err)

			currentSha, err := plugin.Sha256Path(fs, k.Path)
			out.MaybeDie(err, "unable to determine the sha256sum of the current Redpanda Kubernetes plugin %q: %v", k.Path, err)
			if strings.HasPrefix(currentSha, art.Sha256) {
				out.Exit("Redpanda Kubernetes plugin already up-to-date")
			}
			currentVersion, err := k8sPluginVersion(cmd.Context(), k.Path)
			out.MaybeDie(err, "unable to determine current version of the Redpanda Kubernetes plugin: %v", err)

			if !noConfirm {
				latestVersion, err := redpanda.VersionFromString(version)
				out.MaybeDie(err, "unable to parse latest version of the Redpanda Kubernetes plugin: %v", err)
				if latestVersion.Major > currentVersion.Major {
					confirmed, err := out.Confirm("Confirm major version upgrade from %v to %v?", currentVersion.String(), latestVersion.String())
					out.MaybeDie(err, "unable to confirm upgrade: %v", err)
					if !confirmed {
						out.Exit("Upgrade canceled.")
					}
				}
			}

			_, err = downloadAndInstallK8sPlugin(cmd.Context(), fs, pluginDir, art.Path, art.Sha256)
			out.MaybeDieErr(err)
			fmt.Printf("Redpanda Kubernetes plugin successfully upgraded from %v to the latest version (%v).\n", currentVersion.String(), version)
		},
	}
	cmd.Flags().BoolVar(&noConfirm, "no-confirm", false, "Disable confirmation prompt for major version upgrades")
	return cmd
}

// k8sPluginVersion runs the installed plugin with the `version` subcommand and
// parses the version. The operator-built plugin prints "Version: X.Y.Z".
func k8sPluginVersion(ctx context.Context, pluginPath string) (redpanda.Version, error) {
	versionCmd := exec.CommandContext(ctx, pluginPath, "version")
	var sb strings.Builder
	versionCmd.Stdout = &sb
	if err := versionCmd.Run(); err != nil {
		return redpanda.Version{}, err
	}
	var versionStr string
	for l := range strings.SplitSeq(sb.String(), "\n") {
		l = strings.TrimSpace(l)
		if after, ok := strings.CutPrefix(l, "Version: "); ok {
			versionStr = strings.TrimSpace(after)
			break
		}
	}
	version, err := redpanda.VersionFromString(strings.TrimSpace(versionStr))
	if err != nil {
		return redpanda.Version{}, fmt.Errorf("unable to determine Redpanda Kubernetes plugin version from %q: %v", versionStr, err)
	}
	return version, nil
}
