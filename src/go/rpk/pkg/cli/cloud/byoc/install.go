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

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cloudapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/oauth"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/oauth/providers/auth0"
	rpkos "github.com/redpanda-data/redpanda/src/go/rpk/pkg/os"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/plugin"
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
	// First load our configuration and token.
	pluginDir, err := plugin.DefaultBinPath()
	if err != nil {
		return "", "", false, fmt.Errorf("unable to determine managed plugin path: %w", err)
	}
	overrides := cfg.DevOverrides()
	if overrides.CloudToken != "" {
		token = overrides.CloudToken
	} else {
		priorProfile := cfg.ActualProfile()
		_, authVir, clearedProfile, _, err := oauth.LoadFlow(ctx, fs, cfg, auth0.NewClient(cfg.DevOverrides()), false, false, cfg.DevOverrides().CloudAPIURL)
		if err != nil {
			return "", "", false, fmt.Errorf("unable to load the cloud token: %w. You may need to logout with 'rpk cloud logout --clear-credentials' and try again", err)
		}
		oauth.MaybePrintSwapMessage(clearedProfile, priorProfile, authVir)
		token = authVir.AuthToken
	}

	byoc, pluginExists := plugin.ListPlugins(fs, plugin.UserPaths()).Find("byoc")
	zap.L().Debug("looking for existing byoc plugin", zap.Bool("exists", pluginExists))
	// If the plugin exists, and we don't want a version check we want to exit
	// early and avoid calling the Cloud API.
	if c := overrides.BYOCSkipVersionCheck; pluginExists && (c == "1" || c == "true") {
		zap.L().Sugar().Warn("overriding byoc plugin version check. RPK_CLOUD_SKIP_VERSION_CHECK is enabled")
		return byoc.Path, token, false, nil
	}

	// If not, we query the Cloud API for the plugin.
	cloudURL := cloudapi.ProdURL
	if u := overrides.CloudAPIURL; u != "" {
		cloudURL = u
	}
	// Check our current version of the plugin.
	cl := cloudapi.NewClient(cloudURL, token)
	var pack cloudapi.InstallPack
	if isLatest {
		pack, err = cl.LatestInstallPack(ctx)
		if err != nil {
			return "", "", false, fmt.Errorf("unable to get latest installpack: %v", err)
		}
	} else {
		cluster, err := cl.Cluster(ctx, redpandaID)
		if err != nil {
			return "", "", false, fmt.Errorf("unable to request cluster details for %q: %w", redpandaID, err)
		}
		pack, err = cl.InstallPack(ctx, cluster.Spec.InstallPackVersion)
		if err != nil {
			return "", "", false, fmt.Errorf("unable to request install pack details for %q: %v", cluster.Spec.InstallPackVersion, err)
		}
	}

	name := fmt.Sprintf("byoc-%s-%s", runtime.GOOS, runtime.GOARCH)
	artifact, found := pack.Artifacts.Find(name)
	if !found {
		return "", "", false, fmt.Errorf("unable to find byoc plugin %s", name)
	}

	// Unfortunately, the checksum_sha256 returned in the JSON is the
	// sha256 of the plugin *after* tar + gz. We need the checksum of the
	// binary itself.
	//
	// Our current contract is that half the sha256 will be in the filename
	// just before the .tar.gz.
	m := regexp.MustCompile(`\.([a-zA-Z0-9]{16,64})\.tar\.gz$`).FindStringSubmatch(artifact.Location)
	if len(m) == 0 {
		return "", "", false, fmt.Errorf("unable to find sha256 in plugin download filename %q", artifact.Location)
	}
	expShaPrefix := m[1]

	// Check if the plugin is downloaded and matches the remote version. We
	// require the FilenameSHA to have at least 20 characters.
	if pluginExists {
		if !byoc.Managed {
			return "", "", false, fmt.Errorf("found external plugin at %s, the old plugin must be removed first", byoc.Path)
		}
		currentSha, err := plugin.Sha256Path(fs, byoc.Path)
		if err != nil {
			return "", "", false, fmt.Errorf("unable to determine the sha256sum of %q: %v", byoc.Path, err)
		}

		if strings.HasPrefix(currentSha, expShaPrefix) {
			zap.L().Sugar().Debug("version check: installed byoc plugin matches expected version")
			return byoc.Path, token, false, nil // remote version matches, all is good, return token
		}
		zap.L().Sugar().Debug("version check: installed byoc plugin does not match expected version")
	}

	// Remote version is different: download current plugin version and
	// replace.
	zap.L().Debug("downloading byoc plugin", zap.String("version", artifact.Version))
	bin, err := plugin.Download(ctx, artifact.Location, false, expShaPrefix)
	if err != nil {
		return "", "", false, fmt.Errorf("unable to replace out of date plugin: %w", err)
	}

	// Ensure the dir exists, but if we have to create it, we do not want
	// sudo.
	if exists, _ := afero.DirExists(fs, pluginDir); !exists {
		if rpkos.IsRunningSudo() {
			return "", "", false, fmt.Errorf("detected rpk is running with sudo; please execute this command without sudo to avoid saving the plugin as a root owned binary in %s", pluginDir)
		}
		err = os.MkdirAll(pluginDir, 0o755)
		if err != nil {
			return "", "", false, fmt.Errorf("unable to create the plugin bin directory: %v", err)
		}
	}

	zap.L().Sugar().Debugf("writing byoc plugin to %v", pluginDir)
	path, err := plugin.WriteBinary(fs, "byoc", pluginDir, bin, false, true)
	if err != nil {
		return "", "", false, fmt.Errorf("unable to write byoc plugin to disk: %w", err)
	}

	return path, token, true, nil
}
