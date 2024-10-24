// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package container

import (
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/container/common"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newStatusCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	command := &cobra.Command{
		Use:   "status",
		Short: "Get status",
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, _ []string) {
			c, err := common.NewDockerClient(cmd.Context())
			out.MaybeDieErr(err)
			defer c.Close()

			nodes, err := renderClusterInfo(c)
			out.MaybeDieErr(common.WrapIfConnErr(err))

			cfg, err := p.Load(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)

			y, ok := cfg.ActualRpkYaml()
			var withProfile bool
			if ok && y.Profile(common.ContainerProfileName) != nil && y.CurrentProfile == common.ContainerProfileName {
				withProfile = true
			}
			renderClusterInteract(nodes, withProfile)
		},
	}

	return command
}
