// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// Package admin provides a cobra command for the redpanda admin listener.
package admin

import (
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cmd/common"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cmd/redpanda/admin/brokers"
	configcmd "github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cmd/redpanda/admin/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cmd/redpanda/admin/partitions"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

// NewCommand returns the redpanda admin command.
func NewCommand(fs afero.Fs) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "admin",
		Short: "Talk to the Redpanda admin listener",
		Args:  cobra.ExactArgs(0),
	}

	var (
		configFile     string
		hosts          []string
		adminEnableTLS bool
		adminCertFile  string
		adminKeyFile   string
		adminCAFile    string
	)

	cmd.PersistentFlags().StringVar(
		&configFile,
		"config",
		"",
		"rpk config file, if not set the file will be searched for"+
			" in $PWD or /etc/redpanda/redpanda.yaml",
	)

	cmd.PersistentFlags().StringSliceVar(
		&hosts,
		config.FlagAdminHosts1,
		[]string{},
		"A comma-separated list of Admin API addresses (<IP>:<port>)."+
			" You must specify one for each node",
	)

	common.AddAdminAPITLSFlags(
		cmd,
		&adminEnableTLS,
		&adminCertFile,
		&adminKeyFile,
		&adminCAFile,
	)

	cmd.AddCommand(
		brokers.NewCommand(fs),
		partitions.NewCommand(fs),
		configcmd.NewCommand(fs),
	)

	return cmd
}
