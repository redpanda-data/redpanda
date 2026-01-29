// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package secret

import (
	"fmt"

	dataplanev1 "buf.build/gen/go/redpandadata/dataplane/protocolbuffers/go/redpanda/api/dataplane/v1"
	"connectrpc.com/connect"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/publicapi"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newDeleteCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var secretName string

	cmd := &cobra.Command{
		Use:   "delete",
		Short: "Delete an existing secret",
		Long:  "Delete an existing secret from your Redpanda Cloud cluster",
		Run: func(cmd *cobra.Command, _ []string) {
			err := validateSecretName(secretName)
			out.MaybeDie(err, "invalid secret name: %v", err)

			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			if !p.CheckFromCloud() {
				out.Die("this command is only available for cloud clusters")
			}
			var url string
			if p.CloudCluster.IsServerless() && len(p.AdminAPI.Addresses) > 0 {
				url = p.AdminAPI.Addresses[0]
			} else {
				url, err = p.CloudCluster.CheckClusterURL()
				out.MaybeDie(err, "unable to get cluster information: %v", err)
			}
			if url == "" {
				out.Die("unable to setup the client; please login with 'rpk cloud login' and create a cloud profile")
			}
			cl, err := publicapi.NewDataPlaneClientSet(url, p.CurrentAuth().AuthToken)
			out.MaybeDie(err, "unable to initialize cloud client: %v", err)

			request := &dataplanev1.DeleteSecretRequest{
				Id: secretName,
			}
			_, err = cl.Secret.DeleteSecret(cmd.Context(), &connect.Request[dataplanev1.DeleteSecretRequest]{Msg: request})
			out.MaybeDie(err, "unable to delete secret: %v", err)

			fmt.Printf("Secret %s deleted successfully \n", secretName)
		},
	}

	cmd.Flags().StringVar(&secretName, "name", "", "Name of the secret to delete, must be uppercase and can only contain letters, digits, and underscores")
	cmd.MarkFlagRequired("name")

	return cmd
}
