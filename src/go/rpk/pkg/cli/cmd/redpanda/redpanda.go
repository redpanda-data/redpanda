// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

//go:build linux
// +build linux

package redpanda

import (
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cmd/redpanda/admin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cmd/redpanda/tune"
	rp "github.com/redpanda-data/redpanda/src/go/rpk/pkg/redpanda"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func NewCommand(fs afero.Fs, launcher rp.Launcher) *cobra.Command {
	command := &cobra.Command{
		Use:   "redpanda",
		Short: "Interact with a local Redpanda process",
	}

	command.AddCommand(NewStartCommand(fs, launcher))
	command.AddCommand(NewStopCommand(fs))
	command.AddCommand(NewCheckCommand(fs))
	command.AddCommand(NewModeCommand(fs))
	command.AddCommand(NewConfigCommand(fs))

	command.AddCommand(tune.NewCommand(fs))
	command.AddCommand(admin.NewCommand(fs))

	return command
}
