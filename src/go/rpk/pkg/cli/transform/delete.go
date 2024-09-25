// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package transform

import (
	"fmt"

	dataplanev1alpha1 "buf.build/gen/go/redpandadata/dataplane/protocolbuffers/go/redpanda/api/dataplane/v1alpha1"
	"connectrpc.com/connect"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/publicapi"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newDeleteCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var noConfirm bool
	cmd := &cobra.Command{
		Use:   "delete [NAME]",
		Short: "Delete a data transform",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			config.CheckExitServerlessAdmin(p)

			api, err := adminapi.NewClient(cmd.Context(), fs, p)
			out.MaybeDie(err, "unable to initialize admin api client: %v", err)
			functionName := args[0]

			if !noConfirm {
				confirmed, err := out.Confirm("Confirm deletion of transform %q?", functionName)
				out.MaybeDie(err, "unable to confirm deletion: %v", err)
				if !confirmed {
					out.Exit("Deletion canceled.")
				}
			}
			if p.FromCloud && !p.CloudCluster.IsServerless() {
				url, err := p.CloudCluster.CheckClusterURL()
				out.MaybeDie(err, "unable to get cluster information: %v", err)

				cl, err := publicapi.NewDataPlaneClientSet(url, p.CurrentAuth().AuthToken)
				out.MaybeDie(err, "unable to initialize cloud client: %v", err)

				_, err = cl.Transform.DeleteTransform(
					cmd.Context(),
					connect.NewRequest(&dataplanev1alpha1.DeleteTransformRequest{
						Name: functionName,
					}))
				out.MaybeDie(err, "unable to delete transform %q: %v", functionName, err)
			} else {
				err = api.DeleteWasmTransform(cmd.Context(), functionName)
				out.MaybeDie(err, "unable to delete transform %q: %v", functionName, err)
			}
			fmt.Println("Delete successful!")
		},
	}
	cmd.Flags().BoolVar(&noConfirm, "no-confirm", false, "Disable confirmation prompt")
	return cmd
}
