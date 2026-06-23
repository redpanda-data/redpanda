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
	"fmt"
	"os"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/plugin"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

func uninstallCommand(fs afero.Fs) *cobra.Command {
	var (
		force           bool
		includeShadowed bool
	)
	cmd := &cobra.Command{
		Use:   "uninstall",
		Short: "Uninstall the Redpanda Kubernetes plugin",
		Long: `Uninstall the Redpanda Kubernetes plugin.

By default this removes only the rpk-managed plugin binary
(~/.local/bin/.rpk.managed-k8s). If the resolved plugin was not installed by
rpk (a self-managed copy, e.g. one you downloaded or installed via a package
manager), rpk refuses to remove it unless you pass --force.

If the resolved plugin shadows other k8s plugin copies elsewhere on your PATH,
those are left in place unless you pass --include-shadowed.
`,
		Args: cobra.NoArgs,
		Run: func(_ *cobra.Command, _ []string) {
			k, pluginExists := plugin.ListPlugins(fs, plugin.UserPaths()).Find(pluginSlug)
			if !pluginExists {
				out.Exit("The Redpanda Kubernetes plugin is not installed!")
			}

			if refusal := refuseSelfManaged(k, force); refusal != "" {
				out.Die("%s", refusal)
			}
			if len(k.ShadowedPaths) > 0 && !includeShadowed {
				zap.L().Sugar().Warnf("The Redpanda Kubernetes plugin shadows %d other copy/copies left in place: %v. Re-run with --include-shadowed to remove them too.", len(k.ShadowedPaths), k.ShadowedPaths)
			}

			ops, anyFailed := k.Uninstall(includeShadowed)
			tw := out.NewTable("PATH", "MESSAGE")
			defer func() {
				tw.Flush()
				if anyFailed {
					os.Exit(1)
				}
			}()
			for _, o := range ops {
				tw.Print(o.Path, o.Message)
			}
		},
	}
	cmd.Flags().BoolVar(&force, "force", false, "Remove the plugin even if it was not installed by rpk (self-managed)")
	cmd.Flags().BoolVar(&includeShadowed, "include-shadowed", false, "Also remove other k8s plugin copies shadowed by the resolved one")
	return cmd
}

// refuseSelfManaged returns a non-empty refusal message when the resolved
// plugin was not installed by rpk and --force was not supplied. rpk should not
// silently delete a binary it did not install (e.g. a manual rollback copy or
// a package-managed install).
func refuseSelfManaged(p *plugin.Plugin, force bool) string {
	if p.Managed || force {
		return ""
	}
	return fmt.Sprintf("the resolved Redpanda Kubernetes plugin at %q was not installed by rpk (self-managed); refusing to remove it. Re-run with --force to remove it anyway.", p.Path)
}
