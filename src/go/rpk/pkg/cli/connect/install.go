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
	"os"
	"regexp"
	"strings"

	rpkos "github.com/redpanda-data/redpanda/src/go/rpk/pkg/os"
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
		Short: "Install Redpanda Connect",
		Long: `Install Redpanda Connect

This command install the latest version by default.

Alternatively, you may specify a Redpanda Connect version using the 
--connect-version flag.

You may force the installation of Redpanda Connect using the --force flag.
`,
		Args: cobra.NoArgs,
		Run: func(cmd *cobra.Command, _ []string) {
			version = strings.ToLower(version)
			err := validateVersion(version)
			out.MaybeDieErr(err)
			_, installed := plugin.ListPlugins(fs, plugin.UserPaths()).Find("connect")
			if installed && !force {
				if version != "latest" {
					out.Exit("Redpanda connect is already installed. Use --force to force installation, or delete current version with 'rpk connect uninstall' first")
				}
				out.Exit("Redpanda connect is already installed.\nIf you want to upgrade to the latest version, please run 'rpk connect upgrade'.")
			}
			_, installedVersion, err := installConnect(cmd.Context(), fs, version)
			out.MaybeDie(err, "unable to install Redpanda Connect: %v; if running on an air-gapped environment you may install 'redpanda-connect' with your package manager.", err)

			fmt.Printf("Redpanda Connect %v successfully installed.\n", installedVersion)
		},
	}
	cmd.Flags().BoolVar(&force, "force", false, "Force install of Redpanda Connect")
	cmd.Flags().StringVar(&version, "connect-version", "latest", "Redpanda Connect version to install. (e.g. 4.32.0)")
	return cmd
}

// installConnect installs Redpanda Connect plugin in the default location.
// If the plugin is already installed, it will return early and won't download
// the latest plugin. Version string let you select a specific version to
// download, if "latest" or an empty string is passed, it will download the
// latest version.
func installConnect(ctx context.Context, fs afero.Fs, version string) (path, installedVersion string, err error) {
	// We check this before calling the API, to avoid getting the binary
	// and later fail due to a user setting.
	pluginDir, err := plugin.DefaultBinPath()
	if err != nil {
		return "", "", fmt.Errorf("unable to determine plugin default path: %v", err)
	}

	art, ver, err := getConnectArtifact(ctx, version)
	if err != nil {
		return "", "", err
	}
	path, err = downloadAndInstallConnect(ctx, fs, pluginDir, art.Path, art.Sha256)
	return path, ver, err
}

func getConnectArtifact(ctx context.Context, version string) (connectArtifact, string, error) {
	plCl, err := newRepoClient()
	if err != nil {
		return connectArtifact{}, "", err
	}
	manifest, err := plCl.Manifest(ctx)
	if err != nil {
		return connectArtifact{}, "", err
	}
	var (
		art        connectArtifact
		retVersion string
	)
	if version == "latest" || version == "" {
		art, retVersion, err = manifest.LatestArtifact()
		if err != nil {
			return connectArtifact{}, "", err
		}
	} else {
		art, err = manifest.ArtifactVersion(version)
		if err != nil {
			return connectArtifact{}, "", err
		}
		retVersion = version
	}
	return art, retVersion, nil
}

func downloadAndInstallConnect(ctx context.Context, fs afero.Fs, installPath, downloadURL, expShaPrefix string) (string, error) {
	bin, err := plugin.Download(ctx, downloadURL, true, expShaPrefix)
	if err != nil {
		return "", fmt.Errorf("unable to download Redpanda Connect from %q: %v", downloadURL, err)
	}

	// Ensure the directory exists. We ignore errors here because any issues
	// will be handled when we attempt to create the directory with os.MkdirAll.
	if exists, _ := afero.DirExists(fs, installPath); !exists {
		if rpkos.IsRunningSudo() {
			return "", fmt.Errorf("detected rpk is running with sudo; please execute this command without sudo to avoid saving the plugin as a root owned binary in %s", installPath)
		}
		err = os.MkdirAll(installPath, 0o755)
		if err != nil {
			return "", fmt.Errorf("unable to create plugin directory %s: %v", installPath, err)
		}
	}
	zap.L().Sugar().Debugf("writing Redpanda Connect plugin to %v", installPath)
	path, err := plugin.WriteBinary(fs, "connect", installPath, bin, false, true)
	if err != nil {
		return "", fmt.Errorf("unable to write Redpanda Connect plugin: %v", err)
	}
	return path, nil
}

// validateVersion validates that the provided version in the flag is either
// 'latest' or it's a full semantic version.
func validateVersion(version string) error {
	// This simple regexp just matches that a semver is in the string, it may
	// be prefixed with 'v' and contain anything after. Nothing to capture.
	if version == "latest" {
		return nil
	}
	vMatch := regexp.MustCompile(`^v?\d{1,2}\.\d{1,2}\.\d{1,2}`).MatchString(version)
	if !vMatch {
		return fmt.Errorf("provided version %q is not valid. Ensure is either 'latest' or it follows the format MAJOR.MINOR.PATCH (e.g., 2.1.3)", version)
	}
	return nil
}
