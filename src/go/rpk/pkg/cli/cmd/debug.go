// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package cmd

import (
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/cli/cmd/debug"
)

func NewDebugCommand(fs afero.Fs) *cobra.Command {
	command := &cobra.Command{
		Use:   "debug",
		Short: "Debug the local Redpanda process.",
	}
	command.AddCommand(debug.NewInfoCommand(fs))

	debug.AddPlatformDependentCmds(fs, command)

	return command
}
