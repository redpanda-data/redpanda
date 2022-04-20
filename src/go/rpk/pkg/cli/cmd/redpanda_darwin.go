// Copyright 2022 Vectorized, Inc.
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
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func NewRedpandaDarwinCommand(fs afero.Fs) *cobra.Command {
	command := &cobra.Command{
		Use:   "redpanda",
		Short: "Interact with a local or remote Redpanda process",
	}

	command.AddCommand(admin.NewCommand(fs))

	return command
}
