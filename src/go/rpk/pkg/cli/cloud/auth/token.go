// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package auth

import (
	"fmt"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newTokenCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	return &cobra.Command{
		Use:   "token",
		Short: "Print the cloud auth token of the current profile",
		Long: `Print the cloud auth token of the current profile.

This command prints the auth token for the currently selected cloud
authentication to stdout. This is useful for piping the token into other
commands or scripts that need to authenticate with Redpanda Cloud.`,
		Args: cobra.ExactArgs(0),
		Run: func(_ *cobra.Command, _ []string) {
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)

			y, ok := cfg.ActualRpkYaml()
			if !ok {
				out.Die("rpk.yaml file does not exist")
			}

			a := y.CurrentAuth()
			if a == nil {
				out.Die("no current cloud auth selected, use 'rpk cloud login' to log in")
			}
			if a.AuthToken == "" {
				out.Die("current auth has no token, use 'rpk cloud login' to log in")
			}
			fmt.Print(a.AuthToken)
		},
	}
}
