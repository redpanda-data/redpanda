// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// Package selftest contains commands to talk to the Redpanda's admin self_test
// endpoints.
package selftest

import (
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cmd/common"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func NewSelfTestCommand(fs afero.Fs) *cobra.Command {
	var (
		adminURL       string
		adminEnableTLS bool
		adminCertFile  string
		adminKeyFile   string
		adminCAFile    string
	)

	cmd := &cobra.Command{
		Use:   "self-test",
		Short: "Start, stop and query runs of Redpanda self-test through the Admin API listener",
		Args:  cobra.ExactArgs(0),
	}

	common.AddAdminAPITLSFlags(cmd,
		&adminEnableTLS,
		&adminCertFile,
		&adminKeyFile,
		&adminCAFile,
	)

	cmd.AddCommand(
		NewStartCommand(fs),
		NewStopCommand(fs),
		NewStatusCommand(fs),
	)

	cmd.PersistentFlags().StringVar(
		&adminURL,
		config.FlagAdminHosts2,
		"",
		"Comma-separated list of Admin API addresses (<IP>:<port>)")

	return cmd
}
