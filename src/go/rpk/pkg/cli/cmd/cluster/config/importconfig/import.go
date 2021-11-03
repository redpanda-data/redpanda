// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package importconfig

import (
	"fmt"
	"io/ioutil"
	"reflect"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/api/admin"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/config"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/out"
	yaml "gopkg.in/yaml.v3"
)

func ImportConfig(
	client *admin.AdminAPI,
	filename string,
	old_config admin.Config,
	schema admin.ConfigSchema,
	all bool,
) (err error) {
	readback_bytes, err := ioutil.ReadFile(filename)
	out.MaybeDie(err, "Error reading file %s: %v", filename, err)
	var readbackConfig admin.Config
	err = yaml.Unmarshal(readback_bytes, &readbackConfig)
	out.MaybeDie(err, "Error parsing edited config: %v", err)

	// Calculate deltas
	upsert := make(map[string]interface{})
	remove := make([]string, 0)
	for k, v := range readbackConfig {
		oldVal, haveOldVal := old_config[k]
		if meta, ok := schema[k]; ok {
			// For numeric types need special handling because
			// yaml encoding will see '1' as an integer, even
			// if it is given as the value for a floating point
			// ('number') config property, and vice versa.
			if meta.Type == "integer" {
				switch v_float := v.(type) {
				case float64:
					v = int(v_float)
				}

				if oldVal != nil {
					oldVal = int(oldVal.(float64))
				}
			} else if meta.Type == "number" {
				switch x := v.(type) {
				case int:
					v = float64(x)
				}
			} else if meta.Type == "array" && meta.Items.Type == "string" {
				if v != nil {
					v = loadStringArray(v.([]interface{}))
				}
				if oldVal != nil {
					oldVal = loadStringArray(oldVal.([]interface{}))
				}
			}

			// For types that aren't numberic or array, pass them through as-is
		}

		if haveOldVal {
			// If value changed, add it to list of updates
			// DeepEqual because values can be slices
			if !reflect.DeepEqual(oldVal, v) {
				log.Infof("Changed property %s: %v -> %v", k, oldVal, v)
				upsert[k] = v
			}
		} else {
			// Present in input but not original config, insert
			log.Infof("New property %s", k)
			upsert[k] = v
		}
	}

	for k := range old_config {
		if _, found := readbackConfig[k]; !found {
			meta, in_schema := schema[k]
			if !in_schema {
				continue
			}

			if !all && meta.Visibility == "tunable" {
				continue
			}
			log.Infof("Clearing property %s (was %v)", k, old_config[k])
			remove = append(remove, k)
		}
	}

	if len(upsert) == 0 && len(remove) == 0 {
		log.Infof("No changes were made")
		return nil
	}

	// PUT to admin API
	result, err := client.PatchClusterConfig(upsert, remove)
	out.MaybeDie(err, "Error setting config: %v", err)
	fmt.Printf("Updated config, new config version %d\n", result.ConfigVersion)

	return nil
}

func NewCommand(fs afero.Fs, all *bool) *cobra.Command {
	var filename string
	cmd := &cobra.Command{
		Use:   "import",
		Short: "Import cluster configuration from a file",
		Long: `Import cluster-wide configuration properties

Import configuration from a YAML file, usually generated with corresponding 'export' command.
`,
		Run: func(cmd *cobra.Command, args []string) {
			p := config.ParamsFromCommand(cmd)
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			client, err := admin.NewClient(fs, cfg)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			// GET the schema
			schema, err := client.ClusterConfigSchema()
			out.MaybeDie(err, "unable to query config schema: %v", err)

			// GET current config
			var current_config admin.Config
			current_config, err = client.Config()
			out.MaybeDie(err, "unable to query config values: %v", err)

			// Read back template & parse
			err = ImportConfig(client, filename, current_config, schema, *all)
			out.MaybeDie(err, "Error updating config: %v", err)
		},
	}

	cmd.Flags().StringVar(
		&filename,
		"filename",
		"",
		"Full path to file to import, e.g. '/tmp/config.yml'",
	)
	return cmd
}

func loadStringArray(input []interface{}) []string {
	result := make([]string, len(input))
	for i, v := range input {
		result[i] = v.(string)
	}

	return result
}
