// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package cmd

import (
	"vectorized/pkg/cli/cmd/container"
	"vectorized/pkg/config"

	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func NewContainerCommand(fs afero.Fs, mgr config.Manager) *cobra.Command {
	command := &cobra.Command{
		Use:   "container",
		Short: "Manage a local container cluster",
	}

	command.AddCommand(container.Start(fs, mgr))
	command.AddCommand(container.Stop(fs))
	command.AddCommand(container.Purge(fs))

	return command
}
