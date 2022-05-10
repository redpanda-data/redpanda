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
	"errors"
	"fmt"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/api/admin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

func newSetCommand(fs afero.Fs) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "set <key> <value>",
		Short: "Set a single cluster configuration property",
		Long: `Set a single cluster configuration property.

This command is provided for use in scripts.  For interactive editing, or bulk
changes, use the 'edit' and 'import' commands respectively.

If an empty string is given as the value, the property is reset to its default.`,
		Args: cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			key := args[0]
			value := args[1]

			p := config.ParamsFromCommand(cmd)
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			client, err := admin.NewClient(fs, cfg)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			schema, err := client.ClusterConfigSchema()
			out.MaybeDie(err, "unable to query config schema: %v", err)

			meta, ok := schema[key]
			if !ok {
				out.Die("Unknown property %q", key)
			}

			upsert := make(map[string]interface{})
			remove := make([]string, 0)

			// - For scalars, pass string values through to the REST
			// API -- it will give more informative errors than we can
			// about validation.  Special case strings for nullable
			// properties ('null') and for resetting to default ('')
			// - For arrays, make an effort: otherwise the REST API
			// may interpret a scalar string as a list of length 1
			// (via one_or_many_property).

			if meta.Nullable && value == "null" {
				// Nullable types may be explicitly set to null
				upsert[key] = nil
			} else if meta.Type != "string" && (value == "") {
				// Non-string types that receive an empty string
				// are reset to default
				remove = append(remove, key)
			} else if meta.Type == "array" {
				var a []interface{}
				err = yaml.Unmarshal([]byte(value), &a)
				out.MaybeDie(err, "invalid list syntax")
				upsert[key] = a
			} else {
				upsert[key] = value
			}

			result, err := client.PatchClusterConfig(upsert, remove)
			if he := (*admin.HTTPError)(nil); errors.As(err, &he) {
				// Special case 400 (validation) errors with friendly output
				// about which configuration properties were invalid.
				if he.Response.StatusCode == 400 {
					ve, err := formatValidationError(err, he)
					out.MaybeDie(err, "error setting config: %v", err)
					out.Die("No changes were made: %v", ve)
				}
			}

			out.MaybeDie(err, "error setting property: %v", err)
			fmt.Printf("Successfully updated configuration. New configuration version is %d.\n", result.ConfigVersion)
		},
	}

	return cmd
}
