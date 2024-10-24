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
	"math"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

func newGetCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "get [KEY]",
		Short: "Get a cluster configuration property",
		Long: `Get a cluster configuration property.

This command is provided for use in scripts.  For interactive editing, or bulk
output, use the 'edit' and 'export' commands respectively.`,
		Args: cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			key := args[0]

			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			config.CheckExitCloudAdmin(p)

			client, err := adminapi.NewClient(cmd.Context(), fs, p)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			currentConfig, err := client.SingleKeyConfig(cmd.Context(), key)
			out.MaybeDie(err, "unable to query current config: %v", err)

			val, exists := currentConfig[key]
			if !exists {
				out.Die("Property '%s' not found", key)
			} else {
				// currentConfig is the result of json.Unmarshal into a
				// map[string]interface{}. Due to json rules, all numbers
				// are float64. We do not want to print floats for large
				// numbers.
				if f64, ok := val.(float64); ok {
					if math.Mod(f64, 1.0) == 0 {
						val = int64(f64)
					} else {
						val = f64
					}
				}
				// Intentionally bare output, so that the output can be readily
				// consumed in a script.
				bytes, err := yaml.Marshal(val)
				out.MaybeDie(err, "Unexpected non-YAML-encodable value %v", val)
				fmt.Print(string(bytes))
			}
		},
	}

	return cmd
}
