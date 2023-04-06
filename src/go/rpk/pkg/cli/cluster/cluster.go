// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package cluster

import (
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cmd/cluster/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cmd/cluster/license"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cmd/cluster/maintenance"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cmd/cluster/partitions"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cmd/cluster/selftest"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cmd/cluster/storage"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cmd/common"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cmd/group"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func NewCommand(fs afero.Fs) *cobra.Command {
	var (
		brokers        []string
		configFile     string
		user           string
		password       string
		mechanism      string
		enableTLS      bool
		certFile       string
		keyFile        string
		truststoreFile string
	)
	command := &cobra.Command{
		Use:   "cluster",
		Short: "Interact with a Redpanda cluster",
	}
	// backcompat: until we switch to -X, we need these flags.
	common.AddKafkaFlags(
		command,
		&configFile,
		&user,
		&password,
		&mechanism,
		&enableTLS,
		&certFile,
		&keyFile,
		&truststoreFile,
		&brokers,
	)

	offsets := group.NewDescribeCommand(fs)
	offsets.Deprecated = "replaced by 'rpk group describe'"
	offsets.Hidden = true
	offsets.Use = "offsets"
	command.AddCommand(
		newHealthOverviewCommand(fs),
		newLogdirsCommand(fs),
		newMetadataCommand(fs),

		config.NewConfigCommand(fs),
		license.NewLicenseCommand(fs),
		maintenance.NewMaintenanceCommand(fs),
		partitions.NewPartitionsCommand(fs),
		selftest.NewSelfTestCommand(fs),
		storage.NewCommand(fs),
		offsets,
	)

	return command
}
