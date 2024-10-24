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
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/registry/mode"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/registry/schema"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func NewCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "registry",
		Aliases: []string{"sr"},
		Args:    cobra.ExactArgs(0),
		Short:   "Commands to interact with the schema registry",
	}
	cmd.AddCommand(
		compatibilityLevelCommand(fs, p),
		mode.NewCommand(fs, p),
		schema.NewCommand(fs, p),
		subjectCommand(fs, p),
	)
	return cmd
}
