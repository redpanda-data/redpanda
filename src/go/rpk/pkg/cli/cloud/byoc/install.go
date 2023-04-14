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

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cloud/cloudcfg"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cloudapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/oauth"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/oauth/providers/auth0"
	rpkos "github.com/redpanda-data/redpanda/src/go/rpk/pkg/os"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/plugin"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newInstallCommand(fs afero.Fs) *cobra.Command {
	var params cloudcfg.Params
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
			cfg, err := params.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)
			_, _, installed, err := loginAndEnsurePluginVersion(cmd.Context(), fs, cfg, redpandaID)
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
	cmd.Flags().StringVar(&redpandaID, "redpanda-id", "", "The redpanda ID of the cluster you are creating")
	cmd.MarkFlagRequired("redpanda-id")

	cmd.Flags().StringVar(&params.ClientID, cloudcfg.FlagClientID, "", "The client ID of the organization in Redpanda Cloud")
	cmd.Flags().StringVar(&params.ClientSecret, cloudcfg.FlagClientSecret, "", "The client secret of the organization in Redpanda Cloud")
	cmd.MarkFlagsRequiredTogether(cloudcfg.FlagClientID, cloudcfg.FlagClientSecret)
	return cmd
}

func loginAndEnsurePluginVersion(ctx context.Context, fs afero.Fs, cfg *cloudcfg.Config, redpandaID string) (binPath string, token string, installed bool, rerr error) {
	// First load our configuration and token.
	pluginDir, err := plugin.DefaultBinPath()
	if err != nil {
		return "", "", false, fmt.Errorf("unable to determine managed plugin path: %w", err)
	}
	token, err = oauth.LoadFlow(ctx, fs, cfg, auth0.NewClient(cfg))
	if err != nil {
		return "", "", false, fmt.Errorf("unable to load the cloud token: %w", err)
	}

	// Check our current version of the plugin.
	cl := cloudapi.NewClient(cfg.CloudURL, token)
	cluster, err := cl.Cluster(ctx, redpandaID)
	if err != nil {
		return "", "", false, fmt.Errorf("unable to request cluster details for %q: %w", redpandaID, err)
	}
	pack, err := cl.InstallPack(ctx, cluster.Spec.InstallPackVersion)
	if err != nil {
		return "", "", false, fmt.Errorf("unable to request install pack details for %q: %v", cluster.Spec.InstallPackVersion, err)
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
	byoc, pluginExists := plugin.ListPlugins(fs, []string{pluginDir}).Find("byoc")
	if pluginExists {
		if !byoc.Managed {
			return "", "", false, fmt.Errorf("found external plugin at %s, the old plugin must be removed first", byoc.Path)
		}
		if c := cfg.SkipVersionCheck; c == "1" || c == "true" {
			return byoc.Path, token, false, nil
		}
		currentSha, err := plugin.Sha256Path(fs, byoc.Path)
		if err != nil {
			return "", "", false, fmt.Errorf("unable to determine the sha256sum of %q: %v", byoc.Path, err)
		}

		if strings.HasPrefix(currentSha, expShaPrefix) {
			return byoc.Path, token, false, nil // remote version matches, all is good, return token
		}
	}

	// Remote version is different: download current plugin version and
	// replace.
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

	path, err := plugin.WriteBinary(fs, "byoc", pluginDir, bin, false, true)
	if err != nil {
		return "", "", false, fmt.Errorf("unable to write byoc plugin to disk: %w", err)
	}

	return path, token, true, nil
}
