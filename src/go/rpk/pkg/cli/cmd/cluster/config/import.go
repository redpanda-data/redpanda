// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strings"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/api/admin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	yaml "gopkg.in/yaml.v3"
)

func importConfig(
	client *admin.AdminAPI,
	filename string,
	oldConfig admin.Config,
	schema admin.ConfigSchema,
	all bool,
) (err error) {
	readbackBytes, err := os.ReadFile(filename)
	out.MaybeDie(err, "error reading file %s: %v", filename, err)
	var readbackConfig admin.Config
	err = yaml.Unmarshal(readbackBytes, &readbackConfig)
	out.MaybeDie(err, "error parsing edited config: %v", err)

	type propertyDelta struct {
		Property string
		OldValue string
		NewValue string
	}
	var propertyDeltas []propertyDelta

	// Calculate deltas
	upsert := make(map[string]interface{})
	remove := make([]string, 0)
	for k, v := range readbackConfig {
		oldVal, haveOldVal := oldConfig[k]
		if meta, ok := schema[k]; ok {
			// For numeric types need special handling because
			// yaml encoding will see '1' as an integer, even
			// if it is given as the value for a floating point
			// ('number') config property, and vice versa.
			if meta.Type == "integer" {
				switch vFloat := v.(type) {
				case float64:
					v = int(vFloat)
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
				switch vArray := v.(type) {
				case []interface{}:
					// Normal case: user input is a yaml array
					v = loadStringArray(vArray)
				default:
					// Pass, let the server attempt validation
				}
				if oldVal != nil {
					oldVal = loadStringArray(oldVal.([]interface{}))
				}
			}

			// For types that aren't numeric or array, pass them through as-is
		}

		if haveOldVal {
			// If value changed, add it to list of updates
			// DeepEqual because values can be slices
			if !reflect.DeepEqual(oldVal, v) {
				propertyDeltas = append(propertyDeltas, propertyDelta{k, fmt.Sprintf("%v", oldVal), fmt.Sprintf("%v", v)})
				upsert[k] = v
			}
		} else {
			// Present in input but not original config, insert
			upsert[k] = v
			propertyDeltas = append(propertyDeltas, propertyDelta{k, "", fmt.Sprintf("%v", v)})
		}
	}

	for k := range oldConfig {
		if _, found := readbackConfig[k]; !found {
			meta, inSchema := schema[k]
			if !inSchema {
				continue
			}

			if !all && meta.Visibility == "tunable" {
				continue
			}
			oldValue := oldConfig[k]
			propertyDeltas = append(propertyDeltas, propertyDelta{k, fmt.Sprintf("%v", oldValue), ""})
			remove = append(remove, k)
		}
	}

	if len(upsert) == 0 && len(remove) == 0 {
		fmt.Println("No changes were made.")
		return nil
	}

	tw := out.NewTable("PROPERTY", "PRIOR", "NEW")
	for _, pd := range propertyDeltas {
		tw.PrintStructFields(pd)
	}
	tw.Flush()

	// Newline between table and result of write
	fmt.Printf("\n")

	// PUT to admin API
	result, err := client.PatchClusterConfig(upsert, remove)
	if he := (*admin.HttpError)(nil); errors.As(err, &he) {
		// Special case 400 (validation) errors with friendly output
		// about which configuration properties were invalid.
		if he.Response.StatusCode == 400 {
			fmt.Fprint(os.Stderr, formatValidationError(err, he))
			out.Die("No changes were made.")
		}
	}

	// If we didn't handle a structured 400 error, check for other errors.
	out.MaybeDie(err, "error setting config: %v", err)

	fmt.Printf("Successfully updated config, new config version %d.\n", result.ConfigVersion)

	return nil
}

func formatValidationError(err error, http_err *admin.HttpError) string {
	// Output structured validation errors from server
	var validationErrs map[string]string
	bodyErr := json.Unmarshal(http_err.Body, &validationErrs)
	// If no proper JSON body, fall back to generic HTTP error report
	if bodyErr != nil {
		out.MaybeDie(err, "error setting config: %v", err)
	}

	type kv struct{ k, v string }
	var sortedErrs []kv
	for k, v := range validationErrs {
		sortedErrs = append(sortedErrs, kv{k, v})
	}
	sort.Slice(sortedErrs, func(i, j int) bool { return sortedErrs[i].k < sortedErrs[j].k })

	var buf strings.Builder
	fmt.Fprintf(&buf, "Validation errors:\n")
	for _, kv := range sortedErrs {
		fmt.Fprintf(&buf, " * %s: %s\n", kv.k, kv.v)
	}
	fmt.Fprintf(&buf, "\n")

	return buf.String()
}

func newImportCommand(fs afero.Fs, all *bool) *cobra.Command {
	var filename string
	cmd := &cobra.Command{
		Use:   "import",
		Short: "Import cluster configuration from a file.",
		Long: `Import cluster configuration from a file.

Import configuration from a YAML file, usually generated with
corresponding 'export' command.  This downloads the current cluster
configuration, calculates the difference with the YAML file, and
updates any properties that were changed.  If a property is removed
from the YAML file, it is reset to its default value.  `,
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
			currentConfig, err := client.Config()
			out.MaybeDie(err, "unable to query config values: %v", err)

			// Read back template & parse
			err = importConfig(client, filename, currentConfig, schema, *all)
			out.MaybeDie(err, "error updating config: %v", err)
		},
	}

	cmd.Flags().StringVar(
		&filename,
		"filename",
		"",
		"full path to file to import, e.g. '/tmp/config.yml'",
	)
	return cmd
}

func loadStringArray(input []interface{}) []string {
	result := make([]string, len(input))
	for i, v := range input {
		result[i] = fmt.Sprintf("%v", v)
	}

	return result
}
