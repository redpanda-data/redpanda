// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package auth

import (
	"fmt"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newEditCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:               "edit [NAME]",
		Short:             "Edit an rpk auth",
		Args:              cobra.MaximumNArgs(1),
		Hidden:            true,
		ValidArgsFunction: validAuths(fs, p),
		Run: func(*cobra.Command, []string) {
			fmt.Println("edit is deprecated, rpk now fully manages auth fields.")
		},
	}
	return cmd
}
