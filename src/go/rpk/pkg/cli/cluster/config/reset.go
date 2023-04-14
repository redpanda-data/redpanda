// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package config

import (
	"fmt"
	"path/filepath"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

func newForceResetCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var configCacheFile string
	cmd := &cobra.Command{
		Use:   "force-reset [PROPERTY...]",
		Short: "Forcibly clear a cluster configuration property on this node",
		Long: `Forcibly clear a cluster configuration property on this node.

This command is not for general changes to cluster configuration: use this only
when redpanda will not start due to a configuration issue.

If your cluster is working properly and you would like to reset a property
to its default, you may use the 'set' command with an empty string, or
use the 'edit' command and delete the property's line.

This command erases a named property from an internal cache of the cluster
configuration on the local node, so that on next startup redpanda will treat
the setting as if it was set to the default.

WARNING: this should only be used when redpanda is not running.
`,
		Args: cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, propertyNames []string) {
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			dataDir := cfg.Redpanda.Directory

			// Same filename as in redpanda config_manager.cc
			if configCacheFile == "" {
				const cacheFileName = "config_cache.yaml"
				configCacheFile = filepath.Join(dataDir, cacheFileName)
			}

			// Read YAML
			f, err := afero.ReadFile(fs, configCacheFile)
			out.MaybeDie(err, "Couldn't read %q", configCacheFile)

			// Decode YAML
			var content clusterConfig
			err = yaml.Unmarshal(f, &content)
			out.MaybeDie(err, "Couldn't parse %q: %v", configCacheFile, err)

			// Snip out the value we are resetting
			for _, pn := range propertyNames {
				delete(content, pn)
			}

			// Encode output
			outBytes, err := yaml.Marshal(content)
			out.MaybeDie(err, "Serialization error: %v", configCacheFile, err)

			// Write back output
			err = afero.WriteFile(fs, configCacheFile, outBytes, 0o755)
			out.MaybeDie(err, "Couldn't write %q: %v", configCacheFile, err)

			fmt.Println("The property has been successfully removed from the cluster configuration cache. Next time Redpanda starts, the setting will be treated as if it were set to its default value.")
		},
	}

	cmd.Flags().StringVar(
		&configCacheFile,
		"cache-file",
		"",
		"location of configuration cache file (defaults to redpanda data directory)",
	)

	return cmd
}
