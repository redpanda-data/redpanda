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

// anySlice represents a slice of any value type.
type anySlice []any

// A custom unmarshal is needed because go-yaml parse "YYYY-MM-DD" as a full
// timestamp, writing YYYY-MM-DD HH:MM:SS +0000 UTC when encoding, so we are
// going to treat timestamps as strings.
// See: https://github.com/go-yaml/yaml/issues/770

func (s *anySlice) UnmarshalYAML(n *yaml.Node) error {
	replaceTimestamp(n)

	var a []any
	err := n.Decode(&a)
	if err != nil {
		return err
	}
	*s = a
	return nil
}

func newSetCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "set [KEY] [VALUE]",
		Short: "Set a single cluster configuration property",
		Long: `Set a single cluster configuration property.

This command is provided for use in scripts.  For interactive editing, or bulk
changes, use the 'edit' and 'import' commands respectively.

If an empty string is given as the value, the property is reset to its default.`,
		Args: cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			key := args[0]
			value := args[1]

			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			client, err := admin.NewClient(fs, cfg)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			schema, err := client.ClusterConfigSchema(cmd.Context())
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
				var a anySlice
				err = yaml.Unmarshal([]byte(value), &a)
				out.MaybeDie(err, "invalid list syntax")
				upsert[key] = a
			} else {
				upsert[key] = value
			}

			result, err := client.PatchClusterConfig(cmd.Context(), upsert, remove)
			if he := (*admin.HTTPResponseError)(nil); errors.As(err, &he) {
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

			status, err := client.ClusterConfigStatus(cmd.Context(), true)
			out.MaybeDie(err, "unable to check if the cluster needs to be restarted: %v\nCheck the status with 'rpk cluster config status'.", err)
			for _, value := range status {
				if value.Restart {
					fmt.Print("\nCluster needs to be restarted. See more details with 'rpk cluster config status'.\n")
					break
				}
			}
		},
	}

	return cmd
}
