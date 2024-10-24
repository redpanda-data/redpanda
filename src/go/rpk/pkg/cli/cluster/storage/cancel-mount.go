// Copyright 2024 Redpanda Data, Inc.
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
	"strconv"

	"github.com/redpanda-data/common-go/rpadmin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newMountCancel(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "cancel-mount [MIGRATION ID]",
		Aliases: []string{"cancel-unmount"},
		Short:   "Cancels a mount/unmount operation",
		Long: `Cancels a mount/unmount operation on a topic.

Use the migration ID that is emitted when the mount or unmount operation is executed. 
You can also get the migration ID by listing the mount/unmount operations.`,
		Example: `
Cancel a mount/unmount operation
    rpk cluster storage cancel-mount 123
`,
		Args: cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, from []string) {
			pf, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			config.CheckExitCloudAdmin(pf)
			adm, err := adminapi.NewClient(cmd.Context(), fs, pf)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			migrationID, err := strconv.Atoi(from[0])
			out.MaybeDie(err, "invalid migration ID: %v", err)

			err = adm.ExecuteMigration(cmd.Context(), migrationID, rpadmin.MigrationActionCancel)
			out.MaybeDie(err, "unable to cancel the mount/unmount operation: %v", err)
			fmt.Printf("Successfully canceled the operation with ID %v", migrationID)
		},
	}
	return cmd
}
