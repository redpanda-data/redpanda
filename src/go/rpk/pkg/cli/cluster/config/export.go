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
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/api/admin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func exportConfig(
	file *os.File, schema admin.ConfigSchema, config admin.Config, all bool,
) (err error) {
	// Present properties in alphabetical order, providing some pseudo-grouping based on common prefixes
	keys := make([]string, 0, len(schema))
	for k := range schema {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, name := range keys {
		// We exclude cluster_id from exported config to avoid accidental
		// duplication of the ID from one cluster to another
		if name == "cluster_id" {
			continue
		}
		meta := schema[name]
		curValue := config[name]

		// Deprecated settings are never shown to the user for editing
		if meta.Visibility == "deprecated" {
			continue
		}

		// Only show tunables if the user passed --all
		if meta.Visibility == "tunable" && !all {
			continue
		}

		var sb strings.Builder

		// Preface each property with a descriptive comment
		var commentTokens []string

		if len(meta.EnumValues) > 0 {
			commentTokens = append(commentTokens, fmt.Sprintf("one of %s", strings.Join(meta.EnumValues, ", ")))
		} else if meta.Example != "" {
			commentTokens = append(commentTokens, fmt.Sprintf("e.g. '%s'", meta.Example))
		}

		if meta.NeedsRestart {
			commentTokens = append(commentTokens, "restart required")
		}

		if meta.Nullable {
			commentTokens = append(commentTokens, "may be nil")
		}

		commentDetails := ""
		if len(commentTokens) > 0 {
			commentDetails = fmt.Sprintf(" (%s)", strings.Join(commentTokens, ", "))
		}
		sb.WriteString(fmt.Sprintf("\n# %s%s\n", meta.Description, commentDetails))

		// Compose a YAML representation of the property: this is
		// done with simple prints rather than the yaml module, because
		// in either case we have to carefully format values.
		if meta.Type == "array" {
			switch x := curValue.(type) {
			case nil:
				fmt.Fprintf(&sb, "%s:", name)
			case []interface{}:
				if len(x) > 0 {
					fmt.Fprintf(&sb, "%s:\n", name)
					for _, v := range x {
						fmt.Fprintf(&sb, "    - %v\n", v)
					}
				} else {
					fmt.Fprintf(&sb, "%s: []", name)
				}
			default:
				out.Die("Unexpected property value type: %s: %T", name, curValue)
			}
		} else {
			scalarVal := ""
			switch x := curValue.(type) {
			case float64:
				scalarVal = strconv.FormatFloat(x, 'f', -1, 64)
			case string:
				scalarVal = x
			case bool:
				scalarVal = strconv.FormatBool(x)
			case nil:
				// Leave scalarVal empty
			default:
				out.Die("Unexpected property value type: %s: %T", name, curValue)
			}

			if len(scalarVal) > 0 {
				fmt.Fprintf(&sb, "%s: %s\n", name, scalarVal)
			} else {
				fmt.Fprintf(&sb, "%s:\n", name)
			}
		}

		_, err := file.Write([]byte(sb.String()))
		if err != nil {
			return err
		}
	}

	return nil
}

func newExportCommand(fs afero.Fs, p *config.Params, all *bool) *cobra.Command {
	var filename string

	cmd := &cobra.Command{
		Use:   "export",
		Short: "Export cluster configuration",
		Long: `Export cluster configuration.

Writes out a YAML representation of the cluster configuration to a file,
suitable for editing and later applying with the corresponding 'import'
command.

By default, low level tunables are excluded: use the '--all' flag
to include all properties including these low level tunables.
`,
		Run: func(cmd *cobra.Command, _ []string) {
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			client, err := admin.NewClient(fs, cfg)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			// GET the schema
			schema, err := client.ClusterConfigSchema(cmd.Context())
			out.MaybeDie(err, "unable to query config schema: %v", err)

			// GET current config
			var currentConfig admin.Config
			currentConfig, err = client.Config(cmd.Context(), true)
			out.MaybeDie(err, "unable to query current config: %v", err)

			// Generate a yaml template for editing
			var file *os.File
			if filename == "" {
				file, err = os.CreateTemp("/tmp", "config_*.yaml")
				filename = "/tmp/config_*.yaml"
			} else {
				file, err = os.Create(filename)
			}

			out.MaybeDie(err, "unable to create file %q: %v", filename, err)
			err = exportConfig(file, schema, currentConfig, *all)
			out.MaybeDie(err, "failed to write out config %q: %v", file.Name(), err)
			err = file.Close()
			fmt.Printf("Wrote configuration to file %q.\n", file.Name())
			out.MaybeDie(err, "error closing file %q: %v", file.Name(), err)
		},
	}

	cmd.Flags().StringVarP(
		&filename,
		"filename",
		"f",
		"",
		"path to file to export to, e.g. './config.yml'",
	)

	return cmd
}
