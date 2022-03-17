// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package config

import (
	"io/ioutil"
	"os"
	"os/exec"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/api/admin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newEditCommand(fs afero.Fs, all *bool) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "edit",
		Short: "Edit cluster configuration properties.",
		Long: `Edit cluster-wide configuration properties.

This command opens a text editor to modify the cluster's configuration.

Cluster properties are redpanda settings which apply to all nodes in
the cluster.  These are separate to node properties, which are set with
'rpk redpanda config'.

Modified values are written back when the file is saved and the editor
is closed.  Properties which are deleted are reset to their default
values.

By default, low level tunables are excluded: use the '--all' flag
to edit all properties including these tunables.
`,
		Args: cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, _ []string) {
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
			out.MaybeDie(err, "unable to get current config: %v", err)

			// Generate a yaml template for editing
			file, err := ioutil.TempFile("/tmp", "config_*.yaml")
			out.MaybeDie(err, "unable to create temporary file %q: %v", file.Name(), err)
			err = exportConfig(file, schema, currentConfig, *all)
			out.MaybeDie(err, "failed to write out config file %q: %v", file.Name(), err)
			err = file.Close()
			filename := file.Name()
			out.MaybeDie(err, "error closing temporary file %q: %v", filename, err)

			// Launch editor
			editor := os.Getenv("EDITOR")
			if editor == "" {
				const fallbackEditor = "/usr/bin/nano"
				if _, err := os.Stat(fallbackEditor); err != nil {
					out.Die("Please set $EDITOR to use this command")
				} else {
					editor = fallbackEditor
				}
			}

			child := exec.Command(editor, filename)
			child.Stdout = os.Stdout
			child.Stderr = os.Stderr
			child.Stdin = os.Stdin
			err = child.Run()
			out.MaybeDie(err, "Error running editor: %v", err)

			// Read back template & parse
			err = importConfig(client, filename, currentConfig, schema, *all)
			out.MaybeDie(err, "Error updating config: %v", err)
		},
	}
	return cmd
}
