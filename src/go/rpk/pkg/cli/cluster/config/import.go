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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strings"

	"github.com/redpanda-data/common-go/rpadmin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	yaml "gopkg.in/yaml.v3"
)

type formattedError struct {
	s string
}

func (fe *formattedError) Error() string {
	return fe.s
}

// clusterConfig represents a redpanda configuration.
type clusterConfig map[string]any

// A custom unmarshal is needed because go-yaml parse "YYYY-MM-DD" as a full
// timestamp, writing YYYY-MM-DD HH:MM:SS +0000 UTC when encoding, so we are
// going to treat timestamps as strings.
// See: https://github.com/go-yaml/yaml/issues/770

func replaceTimestamp(n *yaml.Node) {
	if len(n.Content) == 0 {
		return
	}
	for _, innerNode := range n.Content {
		if innerNode.Tag == "!!map" {
			replaceTimestamp(innerNode)
		}
		if innerNode.Tag == "!!timestamp" {
			innerNode.Tag = "!!str"
		}
	}
}

func (c *clusterConfig) UnmarshalYAML(n *yaml.Node) error {
	replaceTimestamp(n)

	var a map[string]any
	err := n.Decode(&a)
	if err != nil {
		return err
	}
	*c = a
	return nil
}

func importConfig(
	ctx context.Context,
	client *rpadmin.AdminAPI,
	filename string,
	oldConfig rpadmin.Config,
	oldConfigFull rpadmin.Config,
	schema rpadmin.ConfigSchema,
	all bool,
) (err error) {
	readbackBytes, err := os.ReadFile(filename)
	if err != nil {
		return fmt.Errorf("error reading file %s: %v", filename, err)
	}
	var readbackConfig clusterConfig
	err = yaml.Unmarshal(readbackBytes, &readbackConfig)
	if err != nil {
		return fmt.Errorf("error parsing edited config: %v", err)
	}

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
		oldValVirtual, haveOldValVirtual := oldConfigFull[k]

		if meta, ok := schema[k]; ok {
			// For numeric types need special handling because
			// yaml encoding will see '1' as an integer, even
			// if it is given as the value for a floating point
			// ('number') config property, and vice versa.
			if meta.Type == "integer" {
				if vFloat, ok := v.(float64); ok {
					v = int(vFloat)
				}

				if oldVal != nil {
					oldVal = int(oldVal.(float64))
				}
				if oldValVirtual != nil {
					oldValVirtual = int(oldValVirtual.(float64))
				}
			} else if meta.Type == "number" {
				if vInt, ok := v.(int); ok {
					v = float64(vInt)
				}
			} else if meta.Type == "string" {
				// Some boolean configurations are inherently strings
				// in which case we type switch to string for correct comparison
				// below. yaml loses this type information during export.
				if vBool, ok := v.(bool); ok {
					v = fmt.Sprintf("%v", vBool)
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
				if oldValVirtual != nil {
					oldValVirtual = loadStringArray(oldValVirtual.([]interface{}))
				}
			}

			// For types that aren't numeric or array, pass them through as-is
		}

		// We exclude cluster_id from upsert here and remove below to avoid any
		// accidental duplication of the ID from one cluster to another
		if k == "cluster_id" {
			continue
		}

		addProperty := func(old interface{}) {
			upsert[k] = v

			// If the value is not [secret], the user changed the redacted sentinel
			// value and is changing the secret. We redact the value that we store
			// to our to-be-printed propertyDeltas.
			if meta, ok := schema[k]; ok {
				if v != nil && meta.IsSecret && fmt.Sprintf("%v", v) != "[secret]" {
					v = "[redacted]"
				}
			}
			propertyDeltas = append(propertyDeltas, propertyDelta{k, fmt.Sprintf("%v", old), fmt.Sprintf("%v", v)})
		}

		if haveOldVal {
			// Since the admin endpoint will redact secret fields, ignore any
			// such sentinel strings we've been given, to avoid accidentally
			// setting configs to this value.
			if fmt.Sprintf("%v", oldVal) == "[secret]" && fmt.Sprintf("%v", v) == "[secret]" {
				continue
			}
			// If value changed, add it to list of updates.
			// DeepEqual because values can be slices.
			if !reflect.DeepEqual(oldVal, v) {
				addProperty(oldVal)
			}
		} else {
			// Present in input but not original config, insert if it differs
			// from the Virtual current value (which may be a default)
			if !haveOldValVirtual || !reflect.DeepEqual(oldValVirtual, v) {
				addProperty(oldValVirtual)
			}
		}
	}

	for k := range oldConfigFull {
		if _, found := readbackConfig[k]; !found {
			if k == "cluster_id" {
				// see above
				continue
			}

			meta, inSchema := schema[k]
			if !inSchema {
				continue
			}

			if !all && meta.Visibility == "tunable" {
				continue
			}
			oldValue, found := oldConfig[k]
			if found {
				propertyDeltas = append(propertyDeltas, propertyDelta{k, fmt.Sprintf("%v", oldValue), ""})
				remove = append(remove, k)
			}
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
	result, err := client.PatchClusterConfig(ctx, upsert, remove)
	if he := (*rpadmin.HTTPResponseError)(nil); errors.As(err, &he) {
		// Special case 400 (validation) errors with friendly output
		// about which configuration properties were invalid.
		if he.Response.StatusCode == 400 {
			ve, err := formatValidationError(err, he)
			if err != nil {
				return fmt.Errorf("error setting config: %v", err)
			}
			return &formattedError{ve}
		}
	}

	// If we didn't handle a structured 400 error, check for other errors.
	if err != nil {
		return fmt.Errorf("error setting config: %v", err)
	}

	fmt.Printf("Successfully updated configuration. New configuration version is %d.\n", result.ConfigVersion)

	status, err := client.ClusterConfigStatus(ctx, true)
	out.MaybeDie(err, "unable to check if the cluster needs to be restarted: %v\nCheck the status with 'rpk cluster config status'.", err)
	for _, value := range status {
		if value.Restart {
			fmt.Print("\nCluster needs to be restarted. See more details with 'rpk cluster config status'.\n")
			break
		}
	}

	return nil
}

func formatValidationError(
	err error, httpErr *rpadmin.HTTPResponseError,
) (string, error) {
	// Output structured validation errors from server
	var validationErrs map[string]string
	bodyErr := json.Unmarshal(httpErr.Body, &validationErrs)
	// If no proper JSON body, fall back to generic HTTP error report
	if bodyErr != nil {
		return "", err
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

	return buf.String(), nil
}

func newImportCommand(fs afero.Fs, p *config.Params, all *bool) *cobra.Command {
	var filename string
	cmd := &cobra.Command{
		Use:   "import",
		Short: "Import cluster configuration from a file",
		Long: `Import cluster configuration from a file.

Import configuration from a YAML file, usually generated with
corresponding 'export' command.  This downloads the current cluster
configuration, calculates the difference with the YAML file, and
updates any properties that were changed.  If a property is removed
from the YAML file, it is reset to its default value.  `,
		Args: cobra.NoArgs,
		Run: func(cmd *cobra.Command, _ []string) {
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			config.CheckExitCloudAdmin(p)

			client, err := adminapi.NewClient(cmd.Context(), fs, p)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			// GET the schema
			schema, err := client.ClusterConfigSchema(cmd.Context())
			out.MaybeDie(err, "unable to query config schema: %v", err)

			// GET current config
			currentConfig, err := client.Config(cmd.Context(), false)
			out.MaybeDie(err, "unable to query config values: %v", err)

			currentFullConfig, err := client.Config(cmd.Context(), true)
			out.MaybeDie(err, "unable to query config values: %v", err)

			// Read back template & parse
			err = importConfig(cmd.Context(), client, filename, currentConfig, currentFullConfig, schema, *all)
			if fe := (*formattedError)(nil); errors.As(err, &fe) {
				fmt.Fprint(os.Stderr, err)
				out.Die("No changes were made")
			}
			out.MaybeDie(err, "error updating config: %v", err)
		},
	}

	cmd.Flags().StringVarP(
		&filename,
		"filename",
		"f",
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
