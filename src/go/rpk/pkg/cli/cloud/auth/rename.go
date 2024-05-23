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

func newRenameToCommand(_ afero.Fs, _ *config.Params) *cobra.Command {
	return &cobra.Command{
		Use:     "rename-to [NAME]",
		Short:   "Rename the current rpk auth",
		Aliases: []string{"rename"},
		Args:    cobra.ExactArgs(1),
		Hidden:  true,
		Run: func(*cobra.Command, []string) {
			fmt.Println("rename-to is deprecated, rpk now fully manages auth names.")
		},
	}
}
