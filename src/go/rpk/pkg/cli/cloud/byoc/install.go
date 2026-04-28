// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package byoc

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"runtime"
	"strings"

	byocpluginv1alpha1 "buf.build/gen/go/redpandadata/cloud/protocolbuffers/go/redpanda/api/byocplugin/v1alpha1"
	"connectrpc.com/connect"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/oauth"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/oauth/providers/auth0"
	rpkos "github.com/redpanda-data/redpanda/src/go/rpk/pkg/osutil"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/plugin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/publicapi"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

func newInstallCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var redpandaID string
	cmd := &cobra.Command{
		Use:   "install",
		Short: "Install the BYOC plugin",
		Long: `Install the BYOC plugin

This command downloads the BYOC managed plugin if necessary. The plugin is
installed by default if you try to run a non-install command, but this command
exists if you want to download the plugin ahead of time.
`,
		Args: cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, _ []string) {
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			_, _, installed, err := loginAndEnsurePluginVersion(cmd.Context(), fs, cfg, redpandaID, false) // latest is always false, we only want to install the pinned byoc version when using `rpk cloud byoc install`
			out.MaybeDie(err, "unable to install byoc plugin: %v", err)
			if !installed {
				fmt.Print(`
Your BYOC plugin is currently up to date, avoiding reinstalling!
`)
				return
			}
			fmt.Print(`
BYOC plugin installed successfully!

This plugin supports autocompletion through 'rpk cloud byoc'. If you enable rpk
autocompletion, start a new terminal and tab complete through it!
`)
		},
	}
	cmd.Flags().StringVar(&redpandaID, flagRedpandaID, "", flagRedpandaIDDesc)
	cmd.MarkFlagRequired(flagRedpandaID)
	return cmd
}

func loginAndEnsurePluginVersion(ctx context.Context, fs afero.Fs, cfg *config.Config, redpandaID string, isLatest bool) (binPath string, token string, installed bool, rerr error) {
	token, err := getTokenOrLogin(ctx, fs, cfg)
	if err != nil {
		return "", "", false, err
	}
	path, installed, err := ensurePluginVersion(ctx, fs, cfg, redpandaID, isLatest, token)
	return path, token, installed, err
}

func ensurePluginVersion(ctx context.Context, fs afero.Fs, cfg *config.Config, redpandaID string, isLatest bool, token string) (path string, installed bool, rerr error) {
	pluginDir, err := plugin.DefaultBinPath()
	if err != nil {
		return "", false, fmt.Errorf("unable to determine managed plugin path: %w", err)
	}

	overrides := cfg.DevOverrides()
	byoc, pluginExists := plugin.ListPlugins(fs, plugin.UserPaths()).Find("byoc")
	zap.L().Debug("looking for existing byoc plugin", zap.Bool("exists", pluginExists))

	if pluginExists && (overrides.BYOCSkipVersionCheck == "1" || overrides.BYOCSkipVersionCheck == "true") {
		zap.L().Sugar().Warn("overriding byoc plugin version check. RPK_CLOUD_SKIP_VERSION_CHECK is enabled")
		return byoc.Path, false, nil
	}

	artifact, err := fetchBYOCArtifact(ctx, overrides.PublicAPIURL, token, redpandaID, isLatest)
	if err != nil {
		return "", false, err
	}

	expectedSha, err := extractShaFromLocation(artifact.Location)
	if err != nil {
		return "", false, err
	}

	if pluginExists {
		if !byoc.Managed {
			return "", false, fmt.Errorf("found external plugin at %s, the old plugin must be removed first", byoc.Path)
		}
		currentSha, err := plugin.Sha256Path(fs, byoc.Path)
		if err != nil {
			return "", false, fmt.Errorf("unable to determine the sha256sum of %q: %v", byoc.Path, err)
		}
		if strings.HasPrefix(currentSha, expectedSha) {
			zap.L().Sugar().Debug("version check: installed byoc plugin matches expected version")
			return byoc.Path, false, nil
		}
		zap.L().Sugar().Debug("version check: installed byoc plugin does not match expected version")
	}

	return downloadAndWritePlugin(ctx, fs, pluginDir, artifact, expectedSha)
}

// fetchBYOCArtifact retrieves the BYOC plugin artifact using the PublicAPI
// based on the redpandaID or the latest version if 'isLatest' is true.
func fetchBYOCArtifact(ctx context.Context, apiURL, token, redpandaID string, isLatest bool) (*byocpluginv1alpha1.Artifact, error) {
	cl := publicapi.NewCloudClientSet(apiURL, token)

	if isLatest {
		res, err := cl.BYOCPlugin.ListLatestArtifacts(ctx, connect.NewRequest(&byocpluginv1alpha1.ListLatestArtifactsRequest{
			Filter: &byocpluginv1alpha1.ListLatestArtifactsRequest_Filter{
				Os:   publicapi.OSToBYOCPluginOS(runtime.GOOS),
				Arch: publicapi.ArchToBYOCPluginArch(runtime.GOARCH),
			},
		}))
		if err != nil {
			return nil, err
		}
		artifacts := res.Msg.GetArtifacts()
		if len(artifacts) == 0 {
			return nil, fmt.Errorf("no BYOC plugin artifacts found for redpanda ID %q; OS: %v, Arch: %v", redpandaID, runtime.GOOS, runtime.GOARCH)
		}
		return artifacts[0], nil
	}

	res, err := cl.BYOCPlugin.ListArtifactsByRedpandaID(ctx, connect.NewRequest(&byocpluginv1alpha1.ListArtifactsByRedpandaIDRequest{
		RedpandaId: redpandaID,
		Filter: &byocpluginv1alpha1.ListArtifactsByRedpandaIDRequest_Filter{
			Os:   publicapi.OSToBYOCPluginOS(runtime.GOOS),
			Arch: publicapi.ArchToBYOCPluginArch(runtime.GOARCH),
		},
	}))
	if err != nil {
		return nil, fmt.Errorf("unable to request cluster details for %q: %w", redpandaID, err)
	}
	artifacts := res.Msg.GetArtifacts()
	if len(artifacts) == 0 {
		return nil, fmt.Errorf("no BYOC plugin artifacts found for redpanda ID %q; OS: %v, Arch: %v", redpandaID, runtime.GOOS, runtime.GOARCH)
	}
	return artifacts[0], nil
}

// downloadAndWritePlugin downloads the BYOC plugin artifact, verifies its
// SHA256 checksum, and writes it to the specified plugin directory.
func downloadAndWritePlugin(ctx context.Context, fs afero.Fs, pluginDir string, artifact *byocpluginv1alpha1.Artifact, expectedSha string) (path string, installed bool, rerr error) {
	zap.L().Debug("downloading byoc plugin", zap.String("version", artifact.Version))

	bin, err := plugin.Download(ctx, artifact.Location, false, expectedSha)
	if err != nil {
		return "", false, fmt.Errorf("unable to replace out of date plugin: %w", err)
	}

	if exists, _ := afero.DirExists(fs, pluginDir); !exists {
		if rpkos.IsRunningSudo() {
			return "", false, fmt.Errorf("detected rpk is running with sudo; please execute this command without sudo to avoid saving the plugin as a root owned binary in %s", pluginDir)
		}
		if err := os.MkdirAll(pluginDir, 0o755); err != nil {
			return "", false, fmt.Errorf("unable to create the plugin bin directory: %v", err)
		}
	}

	zap.L().Sugar().Debugf("writing byoc plugin to %v", pluginDir)
	path, err = plugin.WriteBinary(fs, "byoc", pluginDir, bin, false, true)
	if err != nil {
		return "", false, fmt.Errorf("unable to write byoc plugin to disk: %w", err)
	}

	return path, true, nil
}

// getTokenOrLogin retrieves the Cloud token from the config or prompts the user
// to log in if it is not set. It returns the token or an error if the login
// process fails.
func getTokenOrLogin(ctx context.Context, fs afero.Fs, cfg *config.Config) (string, error) {
	tok, err := oauth.LoadCloudToken(ctx, fs, cfg, auth0.NewClient(cfg.DevOverrides()))
	if err != nil {
		return "", fmt.Errorf("unable to load the cloud token: %w. You may need to logout with 'rpk cloud logout --clear-credentials' and try again", err)
	}
	return tok, nil
}

// extractShaFromLocation extracts the sha256 from the plugin name in the
// location URL. The sha256 is expected to be in the filename, just before
// the .tar.gz extension, and it should be at least 16 characters long.
func extractShaFromLocation(location string) (string, error) {
	// Unfortunately, the checksum_sha256 returned in the JSON is the
	// sha256 of the plugin *after* tar + gz. We need the checksum of the
	// binary itself.
	m := regexp.MustCompile(`\.([a-zA-Z0-9]{16,64})\.tar\.gz$`).FindStringSubmatch(location)
	if len(m) == 0 {
		return "", fmt.Errorf("unable to find sha256 in plugin download filename %q", location)
	}
	return m[1], nil
}
