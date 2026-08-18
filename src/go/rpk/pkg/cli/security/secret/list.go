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
	"io"
	"os"
	"strings"

	dataplanev1 "buf.build/gen/go/redpandadata/dataplane/protocolbuffers/go/redpanda/api/dataplane/v1"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/publicapi"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

type secretListItem struct {
	ID     string   `json:"id" yaml:"id"`
	Scopes []string `json:"scopes,omitempty" yaml:"scopes,omitempty"`
}

func newListCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var nameContains string

	cmd := &cobra.Command{
		Use:   "list",
		Short: "List all secrets",
		Long:  "List all secrets in your Redpanda Cloud cluster",
		Run: func(cmd *cobra.Command, _ []string) {
			f := p.Formatter
			if h, ok := f.Help([]secretListItem{}); ok {
				out.Exit(h)
			}
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

			secrets, err := cl.ListAllSecrets(cmd.Context(), &dataplanev1.ListSecretsFilter{
				NameContains: nameContains,
			})
			out.MaybeDie(err, "unable to list secrets: %v", err)

			var items []secretListItem
			for _, secret := range secrets {
				var scopes []string
				for _, scope := range secret.Scopes {
					name, ok := mapScopeToName()[scope]
					if !ok {
						fmt.Fprintf(os.Stderr, "invalid scope: %s\n", scope.String())
						name = "invalid"
					}
					scopes = append(scopes, name)
				}
				items = append(items, secretListItem{
					ID:     secret.Id,
					Scopes: scopes,
				})
			}
			printSecretList(f, items, cmd.OutOrStdout())
		},
	}

	cmd.Flags().StringVar(&nameContains, "name-contains", "", "Substring match on secret name")
	p.InstallFormatFlag(cmd)

	return cmd
}

func printSecretList(f config.OutFormatter, items []secretListItem, w io.Writer) {
	if isText, _, formatted, err := f.Format(items); !isText {
		out.MaybeDie(err, "unable to print in the requested format %q: %v", f.Kind, err)
		fmt.Fprintln(w, formatted)
		return
	}
	tw := out.NewTableTo(w, "NAME", "SCOPES")
	defer tw.Flush()
	for _, item := range items {
		tw.Print(item.ID, strings.Join(item.Scopes, ", "))
	}
}
