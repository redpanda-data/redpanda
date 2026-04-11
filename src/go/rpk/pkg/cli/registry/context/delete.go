// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package context

import (
	"fmt"
	"slices"

	"github.com/spf13/afero"
	"github.com/spf13/cobra"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/schemaregistry"
)

func deleteCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var noConfirm bool
	cmd := &cobra.Command{
		Use:   "delete [CONTEXT]",
		Short: "Delete a schema registry context",
		Long: `Delete a schema registry context.

A context can only be deleted once all subjects within it have been
hard deleted. Soft-deleted subjects still block context deletion. Use
'rpk registry subject delete --permanent' to hard delete subjects first.

The default context "." cannot be deleted.
`,
		Args: cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			name := args[0]
			if len(name) == 0 || name[0] != '.' {
				out.Die("invalid context name %q: context names must start with a '.'", name)
			}

			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)

			cl, err := schemaregistry.NewClient(fs, p)
			out.MaybeDie(err, "unable to initialize schema registry client: %v", err)

			contexts, err := ListContexts(cmd.Context(), cl.Client)
			out.MaybeDie(err, "%v", err)

			if !slices.Contains(contexts, name) {
				out.Die("context %q not found", name)
			}

			if !noConfirm {
				confirmed, err := out.Confirm("Confirm deletion of context %q?", name)
				out.MaybeDie(err, "unable to confirm deletion: %v", err)
				if !confirmed {
					out.Exit("Deletion canceled.")
				}
			}

			err = cl.DeleteContext(cmd.Context(), name)
			out.MaybeDie(err, "unable to delete context %q: %v", name, err)

			fmt.Printf("Successfully deleted context %q.\n", name)
		},
	}
	cmd.Flags().BoolVar(&noConfirm, "no-confirm", false, "Disable confirmation prompt")
	return cmd
}
