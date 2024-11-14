// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package storage

import (
	"fmt"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cluster/storage/recovery"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/publicapi"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func NewCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "storage",
		Short: "Manage the cluster storage",
	}
	cmd.AddCommand(
		recovery.NewCommand(fs, p),
		newMountCommand(fs, p),
		newUnmountCommand(fs, p),
		newMountList(fs, p),
		newMountStatus(fs, p),
		newMountCancel(fs, p),
		newListMountable(fs, p),
	)
	return cmd
}

func createDataplaneClient(p *config.RpkProfile) (*publicapi.DataPlaneClientSet, error) {
	url, err := p.CloudCluster.CheckClusterURL()
	if err != nil {
		return nil, fmt.Errorf("unable to get cluster information from your profile: %v", err)
	}
	cl, err := publicapi.NewDataPlaneClientSet(url, p.CurrentAuth().AuthToken)
	if err != nil {
		return nil, fmt.Errorf("unable to initialize cloud client: %v", err)
	}
	return cl, nil
}
