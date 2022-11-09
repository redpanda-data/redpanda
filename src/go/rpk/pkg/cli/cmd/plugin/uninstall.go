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
	"os"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/plugin"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newUninstallCommand(fs afero.Fs) *cobra.Command {
	var includeShadowed bool
	cmd := &cobra.Command{
		Use:     "uninstall [NAME]",
		Aliases: []string{"rm"},
		Short:   "Uninstall / remove an existing local plugin",
		Long: `Uninstall / remove an existing local plugin.

This command lists locally installed plugins and removes the first plugin that
matches the requested removal. If --include-shadowed is specified, this command
also removes all shadowed plugins of the same name. To remove a command under a
nested namespace ("rpk foo bar"), you can use "foo_bar".
`,

		Args: cobra.ExactArgs(1),

		Run: func(_ *cobra.Command, args []string) {
			name := args[0]
			installed := plugin.ListPlugins(fs, plugin.UserPaths())

			p, ok := installed.Find(name)
			if !ok {
				out.Exit("Plugin %q does not appear to be installed, exiting!", name)
			}

			err := os.Remove(p.Path)
			out.MaybeDie(err, "unable to remove %q: %v", p.Path, err)
			fmt.Printf("Removed %q.\n", p.Path)

			if !includeShadowed {
				return
			}
			for _, shadowed := range p.ShadowedPaths {
				err = os.Remove(shadowed)
				out.MaybeDie(err, "unable to remove shadowed at %q: %v", shadowed, err)
				fmt.Printf("Removed shadowed at %q.\n", shadowed)
			}
		},
	}
	cmd.Flags().BoolVar(&includeShadowed, "include-shadowed", false, "Also remove shadowed plugins that have the same name")
	return cmd
}
