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
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cluster/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cluster/license"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cluster/maintenance"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cluster/partitions"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cluster/quotas"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cluster/selftest"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cluster/storage"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cluster/txn"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/group"
	pkgconfig "github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func NewCommand(fs afero.Fs, p *pkgconfig.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cluster",
		Short: "Interact with a Redpanda cluster",
	}

	offsets := group.NewDescribeCommand(fs, p)
	offsets.Deprecated = "replaced by 'rpk group describe'"
	offsets.Hidden = true
	offsets.Use = "offsets"
	p.InstallKafkaFlags(offsets)

	cmd.AddCommand(
		newHealthOverviewCommand(fs, p),
		newLogdirsCommand(fs, p),
		newMetadataCommand(fs, p),

		config.NewConfigCommand(fs, p),
		license.NewLicenseCommand(fs, p),
		maintenance.NewMaintenanceCommand(fs, p),
		partitions.NewPartitionsCommand(fs, p),
		selftest.NewSelfTestCommand(fs, p),
		storage.NewCommand(fs, p),
		txn.NewCommand(fs, p),
		quotas.NewCommand(fs, p),
		offsets,
	)

	return cmd
}
