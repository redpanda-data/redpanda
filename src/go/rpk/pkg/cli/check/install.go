// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package check

import (
	"context"
	"fmt"
	"strings"

	rpkos "github.com/redpanda-data/redpanda/src/go/rpk/pkg/osutil"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/plugin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/redpanda"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

func installCommand(fs afero.Fs) *cobra.Command {
	var (
		version string
		force   bool
	)
	cmd := &cobra.Command{
		Use:   "install",
		Short: "Install Redpanda Check",
		Long: `Install Redpanda Check

This command installs the latest version by default.

Alternatively, you may specify a version using the --check-version flag.

You may force the installation using the --force flag.
`,
		Args: cobra.NoArgs,
		Run: func(cmd *cobra.Command, _ []string) {
			version = strings.ToLower(version)
			err := validateVersion(version)
			out.MaybeDieErr(err)
			_, installed := plugin.ListPlugins(fs, plugin.UserPaths()).Find("check")
			if installed && !force {
				if version != "latest" {
					out.Exit("Redpanda Check is already installed. Use --force to force installation, or delete current version with 'rpk check uninstall' first")
				}
				out.Exit("Redpanda Check is already installed.\nIf you want to upgrade to the latest version, please run 'rpk check upgrade'.")
			}
			_, installedVersion, err := installCheck(cmd.Context(), fs, version)
			out.MaybeDie(err, "unable to install redpanda check: %v; you may install 'redpanda-check' manually", err)

			fmt.Printf("Redpanda Check %v successfully installed.\n", installedVersion)
		},
	}
	cmd.Flags().BoolVar(&force, "force", false, "Force install of Redpanda Check")
	cmd.Flags().StringVar(&version, "check-version", "latest", "Redpanda Check version to install (e.g. 0.1.0)")
	return cmd
}

func installCheck(ctx context.Context, fs afero.Fs, version string) (path, installedVersion string, err error) {
	pluginDir, err := plugin.DefaultBinPath()
	if err != nil {
		return "", "", fmt.Errorf("unable to determine plugin default path: %v", err)
	}

	art, ver, err := getCheckArtifact(ctx, version)
	if err != nil {
		return "", "", err
	}
	path, err = downloadAndInstallCheck(ctx, fs, pluginDir, art.Path, art.Sha256)
	return path, ver, err
}

func getCheckArtifact(ctx context.Context, version string) (plugin.RepoArtifact, string, error) {
	plCl, err := newRepoClient()
	if err != nil {
		return plugin.RepoArtifact{}, "", err
	}
	manifest, err := plCl.Manifest(ctx)
	if err != nil {
		return plugin.RepoArtifact{}, "", err
	}
	if version == "latest" || version == "" {
		return manifest.LatestArtifact(checkDisplayName)
	}
	art, err := manifest.ArtifactVersion(checkDisplayName, version)
	if err != nil {
		return plugin.RepoArtifact{}, "", err
	}
	return art, version, nil
}

func downloadAndInstallCheck(ctx context.Context, fs afero.Fs, installPath, downloadURL, expShaPrefix string) (string, error) {
	bin, err := plugin.Download(ctx, downloadURL, true, expShaPrefix)
	if err != nil {
		return "", fmt.Errorf("unable to download Redpanda Check from %q: %v", downloadURL, err)
	}

	if exists, _ := afero.DirExists(fs, installPath); !exists {
		if rpkos.IsRunningSudo() {
			return "", fmt.Errorf("detected rpk is running with sudo; please execute this command without sudo to avoid saving the plugin as a root owned binary in %s", installPath)
		}
		if err := fs.MkdirAll(installPath, 0o755); err != nil {
			return "", fmt.Errorf("unable to create plugin directory %s: %v", installPath, err)
		}
	}
	zap.L().Sugar().Debugf("writing Redpanda Check plugin to %v", installPath)
	path, err := plugin.WriteBinary(fs, "check", installPath, bin, false, true)
	if err != nil {
		return "", fmt.Errorf("unable to write Redpanda Check plugin: %v", err)
	}
	return path, nil
}

// validateVersion validates that the provided version flag is either
// 'latest' or a full semantic version (optionally v-prefixed, optionally with
// a prerelease/build suffix such as -rc1, which is forwarded to the manifest
// for lookup; the manifest is the source of truth for what is published).
func validateVersion(version string) error {
	if version == "latest" || redpanda.ValidVersion(version) {
		return nil
	}
	return fmt.Errorf("provided version %q is not valid. Ensure it is either 'latest' or follows the format MAJOR.MINOR.PATCH (e.g., 0.1.0)", version)
}
