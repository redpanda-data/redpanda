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
	"slices"
	"strings"

	"github.com/redpanda-data/common-go/rpadmin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
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

You may also use <key>=<value> notation for setting configuration properties:

  rpk cluster config set log_retention_ms=-1

If an empty string is given as the value, the property is reset to its default.`,
		Args: cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var key, value string
			if len(args) == 1 && strings.Contains(args[0], "=") {
				kv := strings.SplitN(args[0], "=", 2)
				key, value = kv[0], kv[1]
			} else if len(args) == 2 {
				key, value = args[0], args[1]
			} else {
				out.Die("invalid arguments: %v, please use one of 'rpk cluster config set <key> <value>' or 'rpk cluster config set <key>=<value>'", args)
			}

			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			config.CheckExitCloudAdmin(p)

			client, err := adminapi.NewClient(cmd.Context(), fs, p)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			schema, err := client.ClusterConfigSchema(cmd.Context())
			out.MaybeDie(err, "unable to query config schema: %v", err)

			meta, ok := schema[key]

			if !ok {
				// loop over schema, try to find key in the Aliases,
				for _, v := range schema {
					if slices.Contains(v.Aliases, key) {
						meta, ok = v, true
						break
					}
				}
				if !ok {
					out.Die("Unknown property %q", key)
				}
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
			if he := (*rpadmin.HTTPResponseError)(nil); errors.As(err, &he) {
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
