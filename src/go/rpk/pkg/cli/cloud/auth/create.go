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

func newCreateCommand(_ afero.Fs, _ *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:    "create [NAME]",
		Short:  "Create an rpk cloud auth",
		Args:   cobra.ExactArgs(1),
		Hidden: true,
		Run: func(*cobra.Command, []string) {
			fmt.Println("'rpk cloud auth create' is deprecated as a no-op; use 'rpk cloud login' instead.")
		},
	}
	var slice []string
	cmd.Flags().StringSliceVarP(&slice, "set", "s", nil, "A key=value pair to set in the cloud auth")
	cmd.Flags().StringVar(new(string), "description", "", "Optional description of the auth")
	return cmd
}
