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
	"fmt"
	"os"
	"os/exec"

	"github.com/kballard/go-shellquote"
	"github.com/redpanda-data/common-go/rpadmin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newEditCommand(fs afero.Fs, p *config.Params, all *bool) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "edit",
		Short: "Edit cluster configuration properties",
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
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			config.CheckExitCloudAdmin(p)

			client, err := adminapi.NewClient(cmd.Context(), fs, p)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			// GET the schema
			schema, err := client.ClusterConfigSchema(cmd.Context())
			out.MaybeDie(err, "unable to query config schema: %v", err)

			// GET current config

			currentConfig, err := client.Config(cmd.Context(), true)
			out.MaybeDie(err, "unable to get current config: %v", err)

			err = executeEdit(cmd.Context(), client, schema, currentConfig, all)
			out.MaybeDie(err, "unable to edit: %v", err)
		},
	}
	return cmd
}

func executeEdit(
	ctx context.Context,
	client *rpadmin.AdminAPI,
	schema rpadmin.ConfigSchema,
	currentConfig rpadmin.Config,
	all *bool,
) error {
	// Generate a yaml template for editing
	file, err := os.CreateTemp("/tmp", "config_*.yaml")
	filename := file.Name()
	defer func() {
		err := os.Remove(filename)
		if err != nil {
			fmt.Fprintf(os.Stderr, "unable to remove temporary file %q\n", filename)
		}
	}()
	if err != nil {
		return fmt.Errorf("unable to create temporary file %q: %v", filename, err)
	}

	err = exportConfig(file, schema, currentConfig, *all)
	if err != nil {
		return fmt.Errorf("failed to write out config file %q: %v", filename, err)
	}

	err = file.Close()
	if err != nil {
		return fmt.Errorf("error closing temporary file %q: %v", filename, err)
	}

	// Launch editor
	editor := os.Getenv("EDITOR")
	if editor == "" {
		const fallbackEditor = "/usr/bin/nano"
		if _, err := os.Stat(fallbackEditor); err != nil {
			return fmt.Errorf("please set $EDITOR to use this command")
		} else {
			editor = fallbackEditor
		}
	}

	// Fix: Issue #17386
	eArgs, err := editorAndArguments(editor, filename)
	if err != nil {
		return fmt.Errorf("error opening file %s on editor %s: %v", filename, editor, err)
	}
	child := exec.Command(eArgs[0], eArgs[1:]...)
	child.Stdout = os.Stdout
	child.Stderr = os.Stderr
	child.Stdin = os.Stdin
	err = child.Run()
	if err != nil {
		return fmt.Errorf("error running editor: %v", err)
	}

	// Read back template & parse
	err = importConfig(ctx, client, filename, currentConfig, currentConfig, schema, *all)
	if err != nil {
		return fmt.Errorf("error updating config: %v", err)
	}
	return nil
}

// Split the editor environment variable to see if it has editor options e.g. code -w, where editor is "code" and its options is "-w"
// TODO: Open up this method in case other modules require to open the editor and options

func editorAndArguments(editor string, filename string) ([]string, error) {
	eArgs, err := shellquote.Split(editor)
	if err != nil {
		return nil, err
	}
	eArgs = append(eArgs, filename)

	return eArgs, err
}
