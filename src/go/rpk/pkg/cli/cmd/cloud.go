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
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/cli/cmd/cloud"
)

func NewCloudCommand(fs afero.Fs) *cobra.Command {
	command := &cobra.Command{
		Use:    "cloud",
		Short:  "Interact with Vectorized Cloud",
		Hidden: true,
	}

	command.AddCommand(cloud.NewLoginCommand(fs))
	command.AddCommand(cloud.NewLogoutCommand(fs))
	command.AddCommand(cloud.NewGetCommand(fs))

	return command
}
