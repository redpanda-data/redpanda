// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package plugin

import (
	"fmt"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/plugin"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newListCommand(fs afero.Fs) *cobra.Command {
	var local bool

	cmd := &cobra.Command{
		Use:   "list",
		Short: "List all available plugins",
		Long: `List all available plugins.

By default, this command fetches the remote manifest and prints plugins
available for download. Any plugin that is already downloaded is prefixed with
an asterisk. If a locally installed plugin has a different sha256sum as the one
specified in the manifest, or if the sha256sum could not be calculated for the
local plugin, an additional message is printed.

You can specify --local to print all locally installed plugins, as well as
whether you have "shadowed" plugins (the same plugin specified multiple times).
`,

		Args: cobra.ExactArgs(0),
		Run: func(*cobra.Command, []string) {
			installed := plugin.ListPlugins(fs, plugin.UserPaths())

			if local {
				installed.Sort()

				tw := out.NewTable("NAME", "PATH", "SHADOWS")
				defer tw.Flush()

				for _, p := range installed {
					var shadowed string
					if len(p.ShadowedPaths) > 0 {
						shadowed = p.ShadowedPaths[0]
					}
					tw.Print(p.FullName(), p.Path, shadowed)

					if len(p.ShadowedPaths) < 1 {
						continue
					}
					for _, shadowed = range p.ShadowedPaths[1:] {
						tw.Print("", "", shadowed)
					}
				}

				return
			}

			m, err := getManifest()
			out.MaybeDieErr(err)

			tw := out.NewTable("NAME", "DESCRIPTION", "MESSAGE")
			defer tw.Flush()
			for _, entry := range m.Plugins {
				name := entry.Name
				_, entrySha, _ := entry.PathShaForUser()

				var message string

				p, exists := installed.Find(name)
				if exists {
					name = "*" + name

					sha, err := plugin.Sha256Path(fs, p.Path)
					if err != nil {
						message = fmt.Sprintf("unable to calculate local binary sha256: %v", err)
					} else if sha != entrySha {
						message = "local binary sha256 differs from manifest sha256"
					}
				}

				tw.Print(name, entry.Description, message)
			}
		},
	}

	cmd.Flags().BoolVarP(&local, "local", "l", false, "List locally installed plugins and shadowed plugins")

	return cmd
}
