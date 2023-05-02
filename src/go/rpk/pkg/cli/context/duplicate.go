// Copyright 2023 Redpanda Data, Inc.
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

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newDuplicateToCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var (
		from        string
		description string
	)
	cmd := &cobra.Command{
		Use:               "duplicate-to [NAME]",
		Short:             "Duplicate an rpk context to a new name",
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: validContexts(fs, p),
		Run: func(_ *cobra.Command, args []string) {
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			y, ok := cfg.ActualRpkYaml()
			if !ok {
				out.Die("rpk.yaml file does not exist")
			}

			to := args[0]
			if from == "" {
				from = y.CurrentContext
			}
			cx := y.Context(from)
			if cx == nil {
				out.Die("--from context %q does not exist", from)
				return
			}
			if y.Context(to) != nil {
				out.Die("destination context %q already exists", to)
			}

			dup := *cx
			dup.Name = to
			if description != "" {
				dup.Description = description
			}
			if y.CurrentContext == from {
				y.CurrentContext = to
			}
			err = y.Write(fs)
			out.MaybeDieErr(err)

			if y.CurrentContext == to {
				fmt.Printf("Duplicated and set the current context to %q from %q.\n", to, from)
			} else {
				fmt.Printf("Duplicated to context %q from %q.\n", to, from)
			}
		},
	}
	cmd.Flags().StringVarP(&description, "description", "d", "", "Optional description for the new context, otherwise this keeps the old description")
	cmd.Flags().StringVarP(&from, "from", "f", "", "Context to duplicate, otherwise the current context is used")
	return cmd
}
