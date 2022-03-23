// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

//go:build darwin
// +build darwin

package cmd

import (
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cmd/redpanda/admin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	rp "github.com/redpanda-data/redpanda/src/go/rpk/pkg/redpanda"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func NewRedpandaDarwinCommand(
	fs afero.Fs, mgr config.Manager, launcher rp.Launcher,
) *cobra.Command {
	command := &cobra.Command{
		Use:   "redpanda",
		Short: "Interact with a local Redpanda process",
	}

	command.AddCommand(admin.NewCommand(fs))

	return command
}
