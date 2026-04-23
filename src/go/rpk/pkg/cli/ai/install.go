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
	"os"
	"regexp"
	"strings"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/fips"
	rpkos "github.com/redpanda-data/redpanda/src/go/rpk/pkg/osutil"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/plugin"
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
		Short: "Install the Redpanda AI CLI",
		Long: `Install the Redpanda AI CLI.

This command installs the latest version by default.

Alternatively, you may specify an rpai version using the --rpai-version flag.

You may force the installation of rpai using the --force flag.
`,
		Args: cobra.NoArgs,
		Run: func(cmd *cobra.Command, _ []string) {
			maybeExitFIPS()
			version = strings.ToLower(version)
			err := validateVersion(version)
			out.MaybeDieErr(err)
			_, installed := plugin.ListPlugins(fs, plugin.UserPaths()).Find("rpai")
			if installed && !force {
				if version != "latest" {
					out.Exit("The Redpanda AI CLI is already installed. Use --force to force installation, or delete the current version with 'rpk ai uninstall' first")
				}
				out.Exit("The Redpanda AI CLI is already installed.\nIf you want to upgrade to the latest version, please run 'rpk ai upgrade'")
			}
			_, installedVersion, err := installRpai(cmd.Context(), fs, version)
			out.MaybeDie(err, "unable to install the Redpanda AI CLI: %v; if running in an air-gapped environment you may install 'rpai' with your package manager", err)

			fmt.Printf("Redpanda AI CLI %v successfully installed.\n", installedVersion)
		},
	}
	cmd.Flags().BoolVar(&force, "force", false, "Force install of the Redpanda AI CLI")
	cmd.Flags().StringVar(&version, "rpai-version", "latest", "Redpanda AI CLI version to install (e.g. 0.1.2)")
	return cmd
}

// installRpai installs the Redpanda AI CLI plugin in the default location.
// A "latest" or empty version string downloads the latest published version.
func installRpai(ctx context.Context, fs afero.Fs, version string) (path, installedVersion string, err error) {
	pluginDir, err := plugin.DefaultBinPath()
	if err != nil {
		return "", "", fmt.Errorf("unable to determine plugin default path: %v", err)
	}

	art, ver, err := getRpaiArtifact(ctx, version)
	if err != nil {
		return "", "", err
	}
	path, err = downloadAndInstallRpai(ctx, fs, pluginDir, art.Path, art.Sha256)
	return path, ver, err
}

func getRpaiArtifact(ctx context.Context, version string) (rpaiArtifact, string, error) {
	plCl, err := newRepoClient()
	if err != nil {
		return rpaiArtifact{}, "", err
	}
	manifest, err := plCl.Manifest(ctx)
	if err != nil {
		return rpaiArtifact{}, "", err
	}
	var (
		art        rpaiArtifact
		retVersion string
	)
	if version == "latest" || version == "" {
		art, retVersion, err = manifest.LatestArtifact()
		if err != nil {
			return rpaiArtifact{}, "", err
		}
	} else {
		art, err = manifest.ArtifactVersion(version)
		if err != nil {
			return rpaiArtifact{}, "", err
		}
		retVersion = version
	}
	return art, retVersion, nil
}

func downloadAndInstallRpai(ctx context.Context, fs afero.Fs, installPath, downloadURL, expShaPrefix string) (string, error) {
	bin, err := plugin.Download(ctx, downloadURL, true, expShaPrefix)
	if err != nil {
		return "", fmt.Errorf("unable to download the Redpanda AI CLI from %q: %v", downloadURL, err)
	}

	if exists, _ := afero.DirExists(fs, installPath); !exists {
		if rpkos.IsRunningSudo() {
			return "", fmt.Errorf("detected rpk is running with sudo; please execute this command without sudo to avoid saving the plugin as a root owned binary in %s", installPath)
		}
		if err := os.MkdirAll(installPath, 0o755); err != nil {
			return "", fmt.Errorf("unable to create plugin directory %s: %v", installPath, err)
		}
	}
	zap.L().Sugar().Debugf("writing rpai plugin to %v", installPath)
	path, err := plugin.WriteBinary(fs, "rpai", installPath, bin, false, true)
	if err != nil {
		return "", fmt.Errorf("unable to write rpai plugin: %v", err)
	}
	return path, nil
}

// validateVersion validates that the provided version in the flag is either
// 'latest' or it's a full semantic version.
func validateVersion(version string) error {
	if version == "latest" {
		return nil
	}
	vMatch := regexp.MustCompile(`^v?\d{1,2}\.\d{1,2}\.\d{1,2}`).MatchString(version)
	if !vMatch {
		return fmt.Errorf("provided version %q is not valid. Ensure is either 'latest' or it follows the format MAJOR.MINOR.PATCH (e.g., 0.1.2)", version)
	}
	return nil
}

// maybeExitFIPS exits with a clear error if fips is enabled. rpai does not yet
// ship a FIPS build.
func maybeExitFIPS() {
	if fips.IsEnabled() {
		out.Die("the Redpanda AI CLI is not yet available in FIPS mode")
	}
}
