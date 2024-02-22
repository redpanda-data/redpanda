// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package namespace

import (
	"fmt"
	"os"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/oauth"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/oauth/providers/auth0"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/publicapi"
	controlplanev1beta1 "github.com/redpanda-data/redpanda/src/go/rpk/proto/gen/go/redpanda/api/controlplane/v1beta1"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

type createResponse struct {
	Name  string `json:"name" yaml:"name"`
	ID    string `json:"id" yaml:"id"`
	Error string `json:"error,omitempty" yaml:"error,omitempty"`
}

func createCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	return &cobra.Command{
		Use:   "create [NAMES...]",
		Args:  cobra.MinimumNArgs(1),
		Short: "Create a Namespace in Redpanda Cloud",
		Run: func(cmd *cobra.Command, args []string) {
			f := p.Formatter
			if h, ok := f.Help([]createResponse{}); ok {
				out.Exit(h)
			}
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)
			authToken, err := oauth.LoadFlow(cmd.Context(), fs, cfg, auth0.NewClient(cfg.DevOverrides()), false)
			out.MaybeDie(err, "unable to authenticate with Redpanda Cloud: %v", err)
			cl, err := publicapi.NewClientSet(cmd.Context(), cfg.DevOverrides().PublicAPIURL, authToken)
			out.MaybeDie(err, "unable to create the public api client: %v", err)

			var (
				res   []createResponse
				exit1 bool
			)
			for _, name := range args {
				n, err := cl.Namespace.CreateNamespace(cmd.Context(), &controlplanev1beta1.CreateNamespaceRequest{
					Namespace: &controlplanev1beta1.Namespace{
						Name: name,
					},
				})
				if err != nil {
					res = append(res, createResponse{Name: name, Error: err.Error()})
					exit1 = true
				} else {
					res = append(res, createResponse{Name: n.Name, ID: n.Id})
				}
			}
			if exit1 {
				defer os.Exit(1)
			}
			if isText, _, s, err := f.Format(res); !isText {
				out.MaybeDie(err, "unable to print in the required format %q: %v", f.Kind, err)
				fmt.Println(s)
				return
			}
			tw := out.NewTable("name", "id", "error")
			defer tw.Flush()
			for _, r := range res {
				tw.PrintStructFields(r)
			}
		},
	}
}
