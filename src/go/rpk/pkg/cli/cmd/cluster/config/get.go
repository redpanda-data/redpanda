// Copyright 2022 Vectorized, Inc.
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

	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/api/admin"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/config"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/out"
	"gopkg.in/yaml.v3"
)

func newGetCommand(fs afero.Fs) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "get <key>",
		Short: "Get a cluster configuration property",
		Long: `Get a cluster configuration property.

This command is provided for use in scripts.  For interactive editing, or bulk
output, use the 'edit' and 'export' commands respectively.`,
		Args: cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			key := args[0]

			p := config.ParamsFromCommand(cmd)
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			client, err := admin.NewClient(fs, cfg)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			currentConfig, err := client.Config()
			out.MaybeDie(err, "unable to query current config: %v", err)

			val, exists := currentConfig[key]
			if !exists {
				out.Die("Property '%s' not found", key)
			} else {
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
