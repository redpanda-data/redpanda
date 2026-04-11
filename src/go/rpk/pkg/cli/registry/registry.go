// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package registry

import (
	"github.com/spf13/afero"
	"github.com/spf13/cobra"

	srcontext "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/registry/context"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/registry/mode"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/registry/schema"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
)

func NewCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var (
		schemaCtx        string
		skipContextCheck bool
	)
	cmd := &cobra.Command{
		Use:     "registry",
		Aliases: []string{"sr"},
		Args:    cobra.ExactArgs(0),
		Short:   "Commands to interact with the schema registry",
		PersistentPreRun: func(cmd *cobra.Command, _ []string) {
			if schemaCtx == "" {
				return
			}
			err := srcontext.ValidateContext(cmd.Context(), schemaCtx, fs, p, skipContextCheck)
			out.MaybeDie(err, "%v", err)
		},
	}
	cmd.PersistentFlags().StringVar(&schemaCtx, "schema-context", "", "Schema context to use for all registry operations")
	cmd.PersistentFlags().BoolVar(&skipContextCheck, "skip-context-check", false, "Skip the admin API verification of schema context support")
	cmd.AddCommand(
		compatibilityLevelCommand(fs, p, &schemaCtx),
		srcontext.NewCommand(fs, p, &schemaCtx),
		mode.NewCommand(fs, p, &schemaCtx),
		schema.NewCommand(fs, p, &schemaCtx),
		subjectCommand(fs, p, &schemaCtx),
	)
	return cmd
}
