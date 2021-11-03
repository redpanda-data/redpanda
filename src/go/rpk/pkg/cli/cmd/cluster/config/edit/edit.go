// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package edit

import (
	"io/ioutil"
	"os"
	"os/exec"

	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/api/admin"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/cli/cmd/cluster/config/export"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/cli/cmd/cluster/config/importconfig"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/config"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/out"
)

func NewCommand(fs afero.Fs, all *bool) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "edit",
		Short: "Edit cluster configuration.",
		Long: `Edit cluster-wide configuration properties

Cluster-wide properties apply to all nodes in a redpanda cluster.  For
node properties, use the 'rpk config' command.
`,
		Args: cobra.ExactArgs(0),
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
			var currentConfig admin.Config
			currentConfig, err = client.Config()
			out.MaybeDie(err, "unable to get current config: %v", err)

			// Generate a yaml template for editing
			file, err := ioutil.TempFile("/tmp", "config_*.yaml")
			out.MaybeDie(err, "unable to create temporary file: %v", err)
			err = export.ExportConfig(file, schema, currentConfig, *all)
			out.MaybeDie(err, "Failed to write out config: %v", err)
			err = file.Close()
			filename := file.Name()
			file = nil
			out.MaybeDie(err, "error closing temporary file: %v", err)

			// Launch editor
			const FallbackEditor = "/usr/bin/nano"
			editor := os.Getenv("EDITOR")
			if editor == "" {
				if _, err := os.Stat(FallbackEditor); err != nil {
					out.Die("Please set $EDITOR to use this command")
				} else {
					editor = FallbackEditor
				}
			}

			child := exec.Command(editor, filename)
			child.Stdout = os.Stdout
			child.Stderr = os.Stderr
			child.Stdin = os.Stdin
			err = child.Run()
			out.MaybeDie(err, "Error running editor: %v", err)

			// Read back template & parse
			err = importconfig.ImportConfig(client, filename, currentConfig, schema, *all)
			out.MaybeDie(err, "Error updating config: %v", err)
		},
	}
	return cmd
}
