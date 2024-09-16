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
	"fmt"
	"strings"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/plugin"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func upgradeCommand(fs afero.Fs) *cobra.Command {
	return &cobra.Command{
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
			_, err = downloadAndInstallConnect(cmd.Context(), fs, pluginDir, art.Path, art.Sha256)
			out.MaybeDieErr(err)

			fmt.Printf("Redpanda Connect successfully upgraded to the latest version (%v).\n", version)
		},
	}
}
