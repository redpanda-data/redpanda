// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package config

import (
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/cli/cmd/cluster/config/edit"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/cli/cmd/cluster/config/export"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/cli/cmd/cluster/config/importconfig"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/cli/cmd/common"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/config"
)

func NewConfigCommand(fs afero.Fs) *cobra.Command {
	var (
		all            bool
		adminURL       string
		adminEnableTLS bool
		adminCertFile  string
		adminKeyFile   string
		adminCAFile    string
	)

	command := &cobra.Command{
		Use:   "config",
		Short: "Interact with cluster-wide configuration settings",
	}

	command.PersistentFlags().StringVar(
		&adminURL,
		config.FlagAdminHosts2,
		"",
		"Comma-separated list of admin API addresses (<IP>:<port>")

	common.AddAdminAPITLSFlags(command,
		&adminEnableTLS,
		&adminCertFile,
		&adminKeyFile,
		&adminCAFile,
	)

	command.PersistentFlags().BoolVar(
		&all,
		"all",
		false,
		"Include all properties, including tunables.",
	)

	command.AddCommand(importconfig.NewCommand(fs, &all))
	command.AddCommand(export.NewCommand(fs, &all))
	command.AddCommand(edit.NewCommand(fs, &all))

	return command
}
