// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package debug

import (
	"time"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/api/admin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kgo"
)

type bundleParams struct {
	fs                      afero.Fs
	cfg                     *config.Config
	cl                      *kgo.Client
	admin                   *admin.AdminAPI
	logsSince               string
	logsUntil               string
	path                    string
	logsLimitBytes          int
	controllerLogLimitBytes int
	timeout                 time.Duration
}

func NewCommand(fs afero.Fs) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "debug",
		Short: "Debug the local Redpanda process",
	}

	cmd.AddCommand(
		newBundleCommand(fs),
		NewInfoCommand(),
	)

	return cmd
}
