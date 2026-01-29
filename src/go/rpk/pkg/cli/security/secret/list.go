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
	"strings"

	dataplanev1 "buf.build/gen/go/redpandadata/dataplane/protocolbuffers/go/redpanda/api/dataplane/v1"
	"connectrpc.com/connect"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/publicapi"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newListCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var nameContains string

	cmd := &cobra.Command{
		Use:   "list",
		Short: "List all secrets",
		Long:  "List all secrets in your Redpanda Cloud cluster",
		Run: func(cmd *cobra.Command, _ []string) {
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

			request := &dataplanev1.ListSecretsRequest{
				Filter: &dataplanev1.ListSecretsFilter{
					NameContains: nameContains,
				},
			}
			response, err := cl.Secret.ListSecrets(cmd.Context(), connect.NewRequest(request))
			out.MaybeDie(err, "unable to list secrets: %v", err)

			tw := out.NewTable("NAME", "SCOPES")
			defer tw.Flush()
			for _, secret := range response.Msg.Secrets {
				var secretScopes []string
				for _, scope := range secret.Scopes {
					name, ok := mapScopeToName()[scope]
					if !ok {
						fmt.Printf("invalid scope: %s,", scope.String())
						name = "invalid"
					}
					secretScopes = append(secretScopes, name)
				}
				tw.PrintStructFields(struct {
					Name   string
					Scopes string
				}{
					Name:   secret.Id,
					Scopes: strings.Join(secretScopes, ", "),
				})
			}
		},
	}

	cmd.Flags().StringVar(&nameContains, "name-contains", "", "Substring match on secret name")

	return cmd
}
