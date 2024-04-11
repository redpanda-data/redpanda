// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package auth

import (
	"sort"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newListCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	return &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List rpk cloud authentications",
		Args:    cobra.ExactArgs(0),
		Run: func(*cobra.Command, []string) {
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)

			tw := out.NewTable("name", "kind", "organization", "organization-id")
			defer tw.Flush()

			y, ok := cfg.ActualRpkYaml()
			if !ok {
				return
			}

			sort.Slice(y.CloudAuths, func(i, j int) bool {
				// First by organization name, then by org ID, then by name.
				l, r := y.CloudAuths[i], y.CloudAuths[j]
				return l.Organization < r.Organization ||
					(l.Organization == r.Organization && (l.OrgID < r.OrgID ||
						(l.OrgID == r.OrgID && l.Name < r.Name)))
			})

			for i := range y.CloudAuths {
				a := &y.CloudAuths[i]
				name := a.Name
				if a.OrgID == y.CurrentCloudAuthOrgID && a.Kind == y.CurrentCloudAuthKind {
					name += "*"
				}
				tw.Print(name, a.Kind, a.Organization, a.OrgID)
			}
		},
	}
}
