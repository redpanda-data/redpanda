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

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/api/admin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

// clusterConfigN represents a redpanda configuration.
type clusterConfigN map[string]yaml.Node

// A custom unmarshal is needed because go-yaml parse "YYYY-MM-DD" as a full
// timestamp, writing YYYY-MM-DD HH:MM:SS +0000 UTC when encoding, so we are
// going to treat timestamps as strings.
// See: https://github.com/go-yaml/yaml/issues/770

func (c *clusterConfigN) UnmarshalYAML(n *yaml.Node) error {
	replaceTimestamp(n)

	var a map[string]yaml.Node
	err := n.Decode(&a)
	if err != nil {
		return err
	}
	*c = a
	return nil
}

func newLintCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "lint",
		Short: "Remove any deprecated content from redpanda.yaml",
		Long: `Remove any deprecated content from redpanda.yaml.

Deprecated content includes properties which were set via redpanda.yaml
in earlier versions of redpanda, but are now managed via Redpanda's
central configuration store (and via 'rpk cluster config edit').
`,
		Run: func(cmd *cobra.Command, propertyNames []string) {
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			client, err := admin.NewClient(fs, cfg)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			schema, err := client.ClusterConfigSchema(cmd.Context())
			out.MaybeDie(err, "unable to query config schema: %v", err)

			configFile, err := p.LocateConfig(fs)
			// The LocateConfig error type is a full explanation, pass it through
			// without qualification.
			out.MaybeDieErr(err)

			configIn, err := afero.ReadFile(fs, configFile)
			out.MaybeDie(err, "unable to read config file %q: %v", configFile, err)

			// Decode into intermediate yaml.Node representation to preserve comments
			// when writing the linted file back out.
			var inputDoc clusterConfigN
			err = yaml.Unmarshal(configIn, &inputDoc)
			out.MaybeDie(err, "unable to parse config file: %v", err)

			cleanedDoc := make(map[string]yaml.Node)
			var cleanedProperties []string

			for key, node := range inputDoc {
				if key == "redpanda" {
					var cleanedContent []*yaml.Node
					for i := 0; i < len(node.Content); i += 2 {
						keyNode := node.Content[i]
						valNode := node.Content[i+1]
						key := keyNode.Value
						_, isClusterConfig := schema[key]
						if isClusterConfig {
							cleanedProperties = append(cleanedProperties, key)
						} else {
							cleanedContent = append(cleanedContent, keyNode, valNode)
						}
					}
					node.Content = cleanedContent
				}

				cleanedDoc[key] = node
			}

			if len(cleanedProperties) > 0 {
				fmt.Println("Cleaned up cluster configuration properties:")
				for _, key := range cleanedProperties {
					fmt.Printf(" - %s\n", key)
				}
				fmt.Println("") // Blank line
			}

			configOut, err := yaml.Marshal(cleanedDoc)
			out.MaybeDie(err, "error serializing config: %v", err)

			backupFileName := filepath.Base(configFile) + ".bak"
			backupFile := filepath.Join(filepath.Dir(configFile), backupFileName)
			err = afero.WriteFile(fs, backupFile, configIn, 0o755)
			out.MaybeDie(err, "error writing backup config %q: %v", backupFile, err)
			fmt.Printf("Backed up configuration file to %q\n", backupFileName)

			err = afero.WriteFile(fs, configFile, configOut, 0o755)
			out.MaybeDie(err, "error writing config: %v", err)
			fmt.Printf("Rewrote configuration file %q\n", configFile)
		},
	}

	return cmd
}
