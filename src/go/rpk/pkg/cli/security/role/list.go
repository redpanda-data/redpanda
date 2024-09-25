// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package role

import (
	"sort"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

type listResponse struct {
	Roles []string `json:"roles" yaml:"roles"`
}

func listCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var (
		prefix        string
		principalFlag string
	)
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List roles created in Redpanda",
		Example: `
List all roles in Redpanda:
  rpk security role list

List all roles assigned to the user 'red':
  rpk security role list --principal red

List all roles with the prefix "agent-":
  rpk security role list --prefix "agent-"`,
		Aliases: []string{"ls"},
		Args:    cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, _ []string) {
			f := p.Formatter
			if h, ok := f.Help(listResponse{}); ok {
				out.Exit(h)
			}
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			config.CheckExitServerlessAdmin(p)

			cl, err := adminapi.NewClient(cmd.Context(), fs, p)
			out.MaybeDie(err, "unable to initialize admin api client: %v", err)

			principalType, principal := parsePrincipal(principalFlag)
			res, err := cl.Roles(cmd.Context(), prefix, principal, principalType)
			out.MaybeDie(err, "unable to list roles: %v", err)

			roles := []string{}
			for _, r := range res.Roles {
				roles = append(roles, r.Name)
			}
			sort.Slice(roles, func(i, j int) bool { return roles[i] > roles[j] })

			listed := listResponse{Roles: roles}
			if isText, _, s, err := f.Format(listed); !isText {
				out.MaybeDie(err, "unable to print in the required format %q: %v", f.Kind, err)
				out.Exit(s)
			}

			tw := out.NewTable("NAME")
			defer tw.Flush()
			for _, r := range roles {
				tw.Print(r)
			}
		},
	}

	cmd.Flags().StringVar(&prefix, "prefix", "", "Return the roles matching the specified prefix")
	cmd.Flags().StringVar(&principalFlag, "principal", "", "Return the roles matching the specified principal; if no principal prefix is given, `User:` is used")

	return cmd
}
