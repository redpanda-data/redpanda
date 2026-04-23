// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package ai

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
		Short: "Upgrade to the latest Redpanda AI CLI version",
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, _ []string) {
			maybeExitFIPS()
			pluginDir, err := plugin.DefaultBinPath()
			out.MaybeDie(err, "unable to determine managed plugin path: %w", err)
			rpai, pluginExists := plugin.ListPlugins(fs, plugin.UserPaths()).Find("rpai")
			if !pluginExists {
				out.Die("unable to find the Redpanda AI CLI plugin. You may install it running 'rpk ai install'")
			}
			// If the user has a self-installed rpai (e.g. via Homebrew), we
			// cannot cleanly swap it for a managed copy.
			if !rpai.Managed {
				out.Die("found a self-managed Redpanda AI CLI plugin; unfortunately, we cannot upgrade it with this installation. Run rpk ai uninstall && rpk ai install, or to continue managing rpai manually, keep using the rpai package you installed")
			}
			art, version, err := getRpaiArtifact(cmd.Context(), "latest")
			out.MaybeDieErr(err)

			currentSha, err := plugin.Sha256Path(fs, rpai.Path)
			out.MaybeDie(err, "unable to determine the sha256sum of the current Redpanda AI CLI %q: %v", rpai.Path, err)

			if strings.HasPrefix(currentSha, art.Sha256) {
				out.Exit("Redpanda AI CLI already up-to-date")
			}
			currentVersion, err := rpaiVersion(cmd.Context(), rpai.Path)
			out.MaybeDie(err, "unable to determine current version of the Redpanda AI CLI: %v", err)

			if !noConfirm {
				latestVersion, err := redpanda.VersionFromString(version)
				out.MaybeDie(err, "unable to parse latest version of the Redpanda AI CLI: %v", err)
				if latestVersion.Major > currentVersion.Major {
					confirmed, err := out.Confirm("Confirm major version upgrade from %v to %v?", currentVersion.String(), latestVersion.String())
					out.MaybeDie(err, "unable to confirm upgrade: %v", err)
					if !confirmed {
						out.Exit("Upgrade canceled.")
					}
				}
			}

			_, err = downloadAndInstallRpai(cmd.Context(), fs, pluginDir, art.Path, art.Sha256)
			out.MaybeDieErr(err)

			fmt.Printf("Redpanda AI CLI successfully upgraded from %v to the latest version (%v).\n", currentVersion.String(), version)
		},
	}
	cmd.Flags().BoolVar(&noConfirm, "no-confirm", false, "Disable confirmation prompt for major version upgrades")
	return cmd
}

// rpaiVersion executes the rpai plugin with `--version` and parses the current
// version from the output.
func rpaiVersion(ctx context.Context, rpaiPath string) (redpanda.Version, error) {
	versionCmd := exec.CommandContext(ctx, rpaiPath, "--version")
	var sb strings.Builder
	versionCmd.Stdout = &sb
	if err := versionCmd.Run(); err != nil {
		return redpanda.Version{}, err
	}
	// rpai --version output:
	//   rpai version X.Y.Z (commit abc, built at ...)
	// or (goreleaser default):
	//   Version: X.Y.Z
	//   Date: ...
	out := sb.String()
	var versionStr string
	for l := range strings.SplitSeq(out, "\n") {
		l = strings.TrimSpace(l)
		if after, ok := strings.CutPrefix(l, "Version: "); ok {
			versionStr = strings.TrimSpace(after)
			break
		}
		if after, ok := strings.CutPrefix(l, "rpai version "); ok {
			// Pull the first whitespace-separated token.
			fields := strings.Fields(after)
			if len(fields) > 0 {
				versionStr = fields[0]
				break
			}
		}
	}
	version, err := redpanda.VersionFromString(strings.TrimSpace(versionStr))
	if err != nil {
		return redpanda.Version{}, fmt.Errorf("unable to determine Redpanda AI CLI version from %q: %v", versionStr, err)
	}
	return version, nil
}
