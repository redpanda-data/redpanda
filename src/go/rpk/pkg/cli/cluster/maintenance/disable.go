// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package maintenance

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/api/admin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newDisableCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "disable [BROKER-ID]",
		Short: "Disable maintenance mode for a node",
		Long:  `Disable maintenance mode for a node.`,
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			nodeID, err := strconv.Atoi(args[0])
			if err != nil {
				out.MaybeDie(err, "could not parse node id: %s: %v", args[0], err)
			}

			if nodeID < 0 {
				out.Die("invalid node id: %d", nodeID)
			}

			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			client, err := admin.NewClient(fs, cfg)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			b, err := client.Broker(cmd.Context(), nodeID)
			out.MaybeDie(err, "error retrieving broker status. The node %d is likely dead?: %v\n", nodeID, err)

			if b.Maintenance == nil {
				out.Die("maintenance mode not supported or upgrade in progress?")
			} else if !b.Maintenance.Draining {
				out.Exit("Maintenance mode is already disabled for node %d. Check the status with 'rpk cluster maintenance status'.", nodeID)
			}

			err = client.DisableMaintenanceMode(cmd.Context(), nodeID)
			if he := (*admin.HTTPResponseError)(nil); errors.As(err, &he) {
				if he.Response.StatusCode == 404 {
					body, bodyErr := he.DecodeGenericErrorBody()
					if bodyErr == nil {
						out.Die("Not found: %s", body.Message)
					}
				}
			}

			out.MaybeDie(err, "error disabling maintenance mode: %v", err)

			fmt.Printf("Successfully disabled maintenance mode for node %d. Check the status with 'rpk cluster maintenance status'.\n", nodeID)
		},
	}
	return cmd
}
