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
		Short: "Install the Redpanda Kubernetes plugin",
		Long: `Install the Redpanda Kubernetes plugin.

This command installs the latest version by default.

Alternatively, you may specify a version using the --plugin-version flag.

You may force the installation using the --force flag.
`,
		Args: cobra.NoArgs,
		Run: func(cmd *cobra.Command, _ []string) {
			maybeExitFIPS()
			version = strings.ToLower(version)
			err := validateVersion(version)
			out.MaybeDieErr(err)
			_, installed := plugin.ListPlugins(fs, plugin.UserPaths()).Find(pluginSlug)
			if installed && !force {
				if version != "latest" {
					out.Exit("The Redpanda Kubernetes plugin is already installed. Use --force to force installation, or delete the current version with 'rpk k8s uninstall' first")
				}
				out.Exit("The Redpanda Kubernetes plugin is already installed.\nIf you want to upgrade to the latest version, please run 'rpk k8s upgrade'")
			}
			_, installedVersion, err := installK8sPlugin(cmd.Context(), fs, version)
			out.MaybeDie(err, "unable to install the rpk k8s plugin: %v; if running in an air-gapped environment you may install it manually", err)

			fmt.Printf("Redpanda Kubernetes plugin %v successfully installed.\n", installedVersion)

			// The managed binary is written as .rpk.managed-k8s, which does not
			// replace a self-managed .rpk-k8s. If such a copy exists it keeps
			// winning resolution, so `rpk k8s` would run the stale binary
			// despite this "successfully installed" message — warn the user.
			if shadow, shadowed := shadowingSelfManaged(fs); shadowed {
				fmt.Printf("WARNING: a self-managed Redpanda Kubernetes plugin at %q shadows the version just installed; 'rpk k8s' will keep using it. Remove it (e.g. 'rpk k8s uninstall --force', or delete %q) to use the rpk-managed version.\n", shadow, shadow)
			}
		},
	}
	cmd.Flags().BoolVar(&force, "force", false, "Force install of the Redpanda Kubernetes plugin")
	cmd.Flags().StringVar(&version, "plugin-version", "latest", "Plugin version to install (e.g. 25.3.5)")
	return cmd
}

func installK8sPlugin(ctx context.Context, fs afero.Fs, version string) (path, installedVersion string, err error) {
	pluginDir, err := plugin.DefaultBinPath()
	if err != nil {
		return "", "", fmt.Errorf("unable to determine plugin default path: %v", err)
	}
	art, ver, err := getK8sPluginArtifact(ctx, version)
	if err != nil {
		return "", "", err
	}
	path, err = downloadAndInstallK8sPlugin(ctx, fs, pluginDir, art.Path, art.Sha256)
	return path, ver, err
}

func getK8sPluginArtifact(ctx context.Context, version string) (plugin.RepoArtifact, string, error) {
	plCl, err := newRepoClient()
	if err != nil {
		return plugin.RepoArtifact{}, "", err
	}
	manifest, err := plCl.Manifest(ctx)
	if err != nil {
		return plugin.RepoArtifact{}, "", err
	}
	if version == "latest" || version == "" {
		return manifest.LatestArtifact(displayName)
	}
	art, err := manifest.ArtifactVersion(displayName, version)
	if err != nil {
		return plugin.RepoArtifact{}, "", err
	}
	return art, version, nil
}

func downloadAndInstallK8sPlugin(ctx context.Context, fs afero.Fs, installPath, downloadURL, expShaPrefix string) (string, error) {
	bin, err := plugin.Download(ctx, downloadURL, true, expShaPrefix)
	if err != nil {
		return "", fmt.Errorf("unable to download the Redpanda Kubernetes plugin from %q: %v", downloadURL, err)
	}
	if exists, _ := afero.DirExists(fs, installPath); !exists {
		if rpkos.IsRunningSudo() {
			return "", fmt.Errorf("detected rpk is running with sudo; please execute this command without sudo to avoid saving the plugin as a root owned binary in %s", installPath)
		}
		if err := fs.MkdirAll(installPath, 0o755); err != nil {
			return "", fmt.Errorf("unable to create plugin directory %s: %v", installPath, err)
		}
	}
	zap.L().Sugar().Debugf("writing rpk k8s plugin to %v", installPath)
	path, err := plugin.WriteBinary(fs, pluginSlug, installPath, bin, false, true)
	if err != nil {
		return "", fmt.Errorf("unable to write rpk k8s plugin: %v", err)
	}
	return path, nil
}

// validateVersion accepts 'latest' or a MAJOR.MINOR.PATCH prefix (optionally
// v-prefixed); the manifest is the source of truth for what is published.
func validateVersion(version string) error {
	if version == "latest" {
		return nil
	}
	if !regexp.MustCompile(`^v?\d{1,2}\.\d{1,2}\.\d{1,2}`).MatchString(version) {
		return fmt.Errorf("provided version %q is not valid. Ensure it is either 'latest' or it follows the format MAJOR.MINOR.PATCH (e.g., 25.3.5)", version)
	}
	return nil
}

// shadowingSelfManaged reports the path of a self-managed k8s plugin that
// shadows the rpk-managed binary, if any. The managed binary is written as
// .rpk.managed-k8s; a self-managed .rpk-k8s in the same directory sorts first
// and therefore wins resolution, so callers can warn that the freshly
// installed managed binary will not actually be the one rpk executes.
func shadowingSelfManaged(fs afero.Fs) (string, bool) {
	k, ok := plugin.ListPlugins(fs, plugin.UserPaths()).Find(pluginSlug)
	if !ok || k.Managed {
		return "", false
	}
	return k.Path, true
}

// maybeExitFIPS exits with a clear error if FIPS is enabled; the rpk k8s
// plugin does not yet ship a FIPS build (tracked separately, see K8S-851).
func maybeExitFIPS() {
	if fips.IsEnabled() {
		out.Die("the Redpanda Kubernetes plugin is not yet available in FIPS mode")
	}
}
