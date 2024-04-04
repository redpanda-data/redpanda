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
	"context"
	"fmt"
	"sort"
	"strings"

	controlplanev1beta1 "buf.build/gen/go/redpandadata/cloud/protocolbuffers/go/redpanda/api/controlplane/v1beta1"
	"connectrpc.com/connect"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/oauth"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/oauth/providers/auth0"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/publicapi"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

type listResponse struct {
	Name string `json:"name" yaml:"name"`
	ID   string `json:"id" yaml:"id"`
}

func listCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Args:  cobra.ExactArgs(0),
		Short: "List Namespaces in Redpanda Cloud",
		Run: func(cmd *cobra.Command, _ []string) {
			f := p.Formatter
			if h, ok := f.Help([]listResponse{}); ok {
				out.Exit(h)
			}
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			priorProfile := cfg.ActualProfile()
			_, authVir, clearedProfile, _, err := oauth.LoadFlow(cmd.Context(), fs, cfg, auth0.NewClient(cfg.DevOverrides()), false, false, cfg.DevOverrides().CloudAPIURL)
			out.MaybeDie(err, "unable to authenticate with Redpanda Cloud: %v", err)

			oauth.MaybePrintSwapMessage(clearedProfile, priorProfile, authVir)
			authToken := authVir.AuthToken
			cl, err := publicapi.NewControlPlaneClientSet(cfg.DevOverrides().PublicAPIURL, authToken)

			out.MaybeDie(err, "unable to create the public api client: %v", err)

			namespaces, err := listAllNamespaces(cmd.Context(), cl)
			out.MaybeDie(err, "unable to list namespaces: %v", err)

			if isText, _, s, err := f.Format(namespaces); !isText {
				out.MaybeDie(err, "unable to print in the required format %q: %v", f.Kind, err)
				fmt.Println(s)
				return
			}
			tw := out.NewTable("name", "id")
			defer tw.Flush()
			for _, n := range namespaces {
				tw.PrintStructFields(n)
			}
		},
	}
}

// listAllNamespaces uses the pagination feature to traverse all pages of the
// list request and return all namespaces.
func listAllNamespaces(ctx context.Context, cl *publicapi.ControlPlaneClientSet) ([]listResponse, error) {
	var (
		pageToken string
		listed    []*controlplanev1beta1.Namespace
	)
	for {
		l, err := cl.Namespace.ListNamespaces(ctx, connect.NewRequest(&controlplanev1beta1.ListNamespacesRequest{PageToken: pageToken}))
		if err != nil {
			return nil, err
		}
		listed = append(listed, l.Msg.Namespaces...)
		if pageToken = l.Msg.NextPageToken; pageToken == "" {
			break
		}
	}
	var res []listResponse
	for _, n := range listed {
		if n != nil {
			res = append(res, listResponse{n.Name, n.Id})
		}
	}
	sort.Slice(res, func(i, j int) bool { return strings.ToLower(res[i].Name) < strings.ToLower(res[j].Name) })
	return res, nil
}
