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
	"os"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/plugin"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newUninstallCommand(fs afero.Fs) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "uninstall",
		Short: "Uninstall the BYOC plugin",
		Long: `Uninstall the BYOC plugin

This command deletes your locally downloaded BYOC managed plugin if it exists.
Often, you only need to download the plugin to create your cluster once, and
then you never need the plugin again. You can uninstall to save a small bit of
disk space.
`,
		Args: cobra.ExactArgs(0),
		Run: func(_ *cobra.Command, _ []string) {
			pluginDir, err := plugin.DefaultBinPath()
			out.MaybeDie(err, "unable to determine managed plugin path: %w", err)
			byoc, pluginExists := plugin.ListPlugins(fs, []string{pluginDir}).Find("byoc")
			if !pluginExists {
				out.Exit("The BYOC managed plugin is not installed!")
			}
			messages, anyFailed := byoc.Uninstall(true)
			tw := out.NewTable("PATH", "MESSAGE")
			defer func() {
				tw.Flush()
				if anyFailed {
					os.Exit(1)
				}
			}()
			for _, m := range messages {
				tw.Print(m.Path, m.Message)
			}
		},
	}
	return cmd
}
