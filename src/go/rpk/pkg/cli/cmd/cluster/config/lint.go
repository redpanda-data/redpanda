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
	"path/filepath"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/api/admin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

func newLintCommand(fs afero.Fs) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "lint",
		Short: "Remove any deprecated content from redpanda.yaml.",
		Long: `Remove any deprecated content from redpanda.yaml.

Deprecated content includes properties which were set via redpanda.yaml
in earlier versions of redpanda, but are now managed via Redpanda's
central configuration store (and via 'rpk cluster config edit').
`,
		Run: func(cmd *cobra.Command, propertyNames []string) {
			p := config.ParamsFromCommand(cmd)
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			client, err := admin.NewClient(fs, cfg)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			schema, err := client.ClusterConfigSchema()
			out.MaybeDie(err, "unable to query config schema: %v", err)

			configFile, err := p.LocateConfig(fs)
			// The LocateConfig error type is a full explanation, pass it through
			// without qualification.
			out.MaybeDieErr(err)

			configIn, err := afero.ReadFile(fs, configFile)
			out.MaybeDie(err, "unable to read config file %q: %v", configFile, err)

			// Decode into intermediate yaml.Node representation to preserve comments
			// when writing the linted file back out.
			inputDoc := make(map[string]yaml.Node)
			err = yaml.Unmarshal(configIn, inputDoc)
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
				log.Info("Cleaned up cluster configuration properties:")
				for _, key := range cleanedProperties {
					log.Infof(" - %s", key)
				}
				log.Info("") // Blank line
			}

			configOut, err := yaml.Marshal(cleanedDoc)
			out.MaybeDie(err, "error serializing config: %v", err)

			backupFileName := filepath.Base(configFile) + ".bak"
			backupFile := filepath.Join(filepath.Dir(configFile), backupFileName)
			err = afero.WriteFile(fs, backupFile, configIn, 0755)
			out.MaybeDie(err, "error writing backup config %q: %v", backupFile, err)
			log.Infof("Backed up configuration file to %q", backupFileName)

			err = afero.WriteFile(fs, configFile, configOut, 0755)
			out.MaybeDie(err, "error writing config: %v", err)
			log.Infof("Rewrote configuration file %q", configFile)
		},
	}

	return cmd
}
