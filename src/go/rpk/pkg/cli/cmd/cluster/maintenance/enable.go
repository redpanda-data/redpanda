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
	"time"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/api/admin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newEnableCommand(fs afero.Fs) *cobra.Command {
	var (
		wait bool
	)
	cmd := &cobra.Command{
		Use:   "enable <node-id>",
		Short: "Enable maintenance mode for a node.",
		Long: `Enable maintenance mode for a node.

This command enables maintenance mode for the node with the specified ID. If a
node exists that is already in maintenance mode then an error will be returned.
`,
		Args: cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			nodeId, err := strconv.Atoi(args[0])
			if err != nil {
				out.MaybeDie(err, "could not parse node id: %s: %v", args[0], err)
			}

			if nodeId < 0 {
				out.Die("invalid node id: %d", nodeId)
			}

			p := config.ParamsFromCommand(cmd)
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			client, err := admin.NewClient(fs, cfg)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			err = client.EnableMaintenanceMode(nodeId)
			var he *admin.HttpError
			if errors.As(err, &he) {
				if he.Response.StatusCode == 404 {
					body, bodyErr := he.DecodeGenericErrorBody()
					if bodyErr == nil {
						out.Die("Not found: %s", body.Message)
					}
				} else if he.Response.StatusCode == 400 {
					body, bodyErr := he.DecodeGenericErrorBody()
					if bodyErr == nil {
						out.Die("Cannot enable maintenance mode: %s", body.Message)
					}
				}
			}

			out.MaybeDie(err, "error enabling maintenance mode for node %d: %v", nodeId, err)
			fmt.Printf("Successfully enabled maintenance mode for node %d\n", nodeId)

			if !wait {
				return
			}

			fmt.Println("Waiting for node to drain...")

			var table *out.TabWriter
			retries := 3
			for {
				b, err := client.Broker(nodeId)
				if err == nil && b.Maintenance == nil {
					err = fmt.Errorf("maintenance mode not supported. upgrade in progress?")
					// since admin api client uses `sendAny` it is possible that
					// we enabled maintenance mode and then started querying a
					// broker that hadn't been upgraded yet.  with the retry,
					// this is unlikely. and won't occur at all once all nodes
					// are upgraded to v22.1.x.
					//
					// [[fallthrough]].
				}
				if err != nil {
					if retries <= 0 {
						out.Die("Error retrieving broker status: %v", err)
					}
					retries--
					time.Sleep(time.Second * 2)
					continue
				}
				retries = 3
				if table == nil {
					table = newMaintenanceReportTable()
				}
				addBrokerMaintenanceReport(table, b)
				table.Flush()
				if b.Maintenance.Draining && b.Maintenance.Finished {
					return
				}
				time.Sleep(time.Second * 2)
			}
		},
	}
	cmd.Flags().BoolVarP(&wait, "wait", "w", false, "Wait until node is drained")
	return cmd
}
