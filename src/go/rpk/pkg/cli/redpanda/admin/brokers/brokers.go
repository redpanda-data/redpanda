// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// Package brokers contains commands to talk to the Redpanda's admin brokers
// endpoints.
package brokers

import (
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

// NewCommand returns the brokers admin command.
func NewCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "brokers",
		Short: "View and configure Redpanda brokers through the admin listener",
		Args:  cobra.ExactArgs(0),
	}
	cmd.AddCommand(
		newListCommand(fs, p),
		newDecommissionBroker(fs, p),
		newDecommissionBrokerStatus(fs, p),
		newRecommissionBroker(fs, p),
	)
	return cmd
}
