// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package remotebundle

import (
	"errors"
	"strings"

	"github.com/redpanda-data/common-go/rpadmin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func NewCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "remote-bundle",
		Short: "Create a debug bundle from a remote cluster",
		Long: `Create a debug bundle from a remote cluster.

The remote-bundle commands can be used to get a debug bundle of your remote 
cluster configured in flags, environment variables, or your rpk profile.
`,
		Args: cobra.NoArgs,
	}
	cmd.AddCommand(
		newStartCommand(fs, p),
		newStatusCommand(fs, p),
		newDownloadCommand(fs, p),
		newCancelCommand(fs, p),
	)
	return cmd
}

func printBrokers(addresses []string) {
	tw := out.NewTable("broker")
	defer tw.Flush()
	for _, address := range addresses {
		tw.Print(address)
	}
}

func tryMessageFromErr(err error) string {
	if he := (*rpadmin.HTTPResponseError)(nil); errors.As(err, &he) {
		if body, err := he.DecodeGenericErrorBody(); err == nil {
			return body.Message
		}
	}
	return strings.TrimSpace(err.Error())
}

func isAlreadyStartedErr(err error) bool {
	if he := (*rpadmin.HTTPResponseError)(nil); errors.As(err, &he) {
		if body, err := he.DecodeGenericErrorBody(); err == nil {
			return body.Code == rpadmin.DebugBundleErrorCodeProcessAlreadyRunning
		}
	}
	return false
}
