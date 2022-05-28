// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package registry

import (
	"github.com/spf13/cobra"
)

func flagURLs(cmd *cobra.Command, urls *[]string) {
	cmd.Flags().StringSliceVar(urls, "urls", []string{"localhost:7072"}, "schema registry urls")
}

func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "registry",
		Args:  cobra.ExactArgs(0),
		Short: "Commands to interact with the schema registry.",
	}

	cmd.AddCommand(
		subjectCommand(),
		schemaCommand(),
		compatibilityLevelCommand(),
	)
	return cmd
}
