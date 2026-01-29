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
	"errors"
	"fmt"
	"strings"

	dataplanev1 "buf.build/gen/go/redpandadata/dataplane/protocolbuffers/go/redpanda/api/dataplane/v1"
	"connectrpc.com/connect"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/publicapi"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
)

func newUpdateCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var secretName, secretValue string
	var scopes []string

	cmd := &cobra.Command{
		Use:   "update",
		Short: "Update an existing secret",
		Long: `Update an existing secret for your Redpanda Cloud cluster.

Scopes define the areas where the secret can be used. Updating a secret will 
overwrite its scopes. Available scope options are: redpanda_connect, redpanda_cluster`,
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

			var scopeRequest []dataplanev1.Scope
			for _, scope := range scopes {
				vs, ok := mapNameToScope()[scope]
				if !ok {
					out.Die("invalid scope: %s, available options are: %s", scope, strings.Join(getScopeNames(), ", "))
				}
				scopeRequest = append(scopeRequest, vs)
			}

			request := &dataplanev1.UpdateSecretRequest{
				Id:         strings.ToUpper(secretName),
				SecretData: []byte(secretValue),
				Scopes:     scopeRequest,
			}

			response, err := cl.Secret.UpdateSecret(cmd.Context(), connect.NewRequest(request))
			if err != nil {
				var connectErr *connect.Error
				if errors.As(err, &connectErr) {
					if connectErr.Code() == connect.CodeAlreadyExists {
						out.Die("secret %s already exists", secretName)
					}
					if connectErr.Code() == connect.CodeNotFound {
						out.Die("secret %s not found", secretName)
					}
					if connectErr.Code() == connect.CodeInvalidArgument {
						for _, detail := range connectErr.Details() {
							c, _ := detail.Value()
							switch d := c.(type) {
							case *errdetails.BadRequest:
								for _, violation := range d.FieldViolations {
									out.Die(fmt.Sprintf("invalid field:%s, error=%s\n",
										violation.Field, violation.Description))
								}
							default:
								// do nothing
							}
						}
					}
				}
				out.MaybeDie(err, "unable to create secret: %v", err)
			}
			fmt.Printf("Secret %s updated successfully \n", response.Msg.Secret.Id)
		},
	}

	cmd.Flags().StringVar(&secretName, "name", "", "Name of the secret, must be uppercase and can only contain letters, digits, and underscores")
	cmd.Flags().StringVar(&secretValue, "value", "", "New secret value of the secret")
	cmd.Flags().StringSliceVar(&scopes, "scopes", nil, "Scope of the secret (e.g. redpanda_connect)")
	cmd.MarkFlagRequired("name")
	cmd.MarkFlagRequired("value")
	cmd.MarkFlagRequired("scopes")

	cmd.RegisterFlagCompletionFunc("scopes", func(_ *cobra.Command, _ []string, _ string) ([]string, cobra.ShellCompDirective) {
		return getScopeNames(), cobra.ShellCompDirectiveNoSpace
	})

	return cmd
}
