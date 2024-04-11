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
	"fmt"
	"sort"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newDeleteCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete [NAME]",
		Short: "Delete an rpk cloud authentication",
		Long: `Delete an rpk cloud authentication.

Deleting a cloud authentication removes it from the rpk.yaml file. If the
deleted auth was the current authentication, rpk will use a default SSO auth the
next time you try to login and save that auth.

If you delete an authentication that is used by profiles, affected profiles have
their authentication cleared and you will only be able to access the profile's
cluster using SASL credentials.
`,
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: validAuths(fs, p),
		Run: func(_ *cobra.Command, args []string) {
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)

			y, ok := cfg.ActualRpkYaml()
			if !ok {
				out.Die("rpk.yaml file does not exist")
			}

			name := args[0]

			nameMatches := findName(y, name)
			if len(nameMatches) == 0 {
				out.Die("cloud auth %q does not exist", name)
			}

			// We could have deleted multiple if there is a bug in
			// the logic or if the file is corrupted somehow; we do
			// exact name match and should prevent creation of
			// duplicates. But, if we delete multiple, that's ok;
			// we just print information about the first.
			keep := y.CloudAuths[:0]
			var deleted config.RpkCloudAuth
			for i := range y.CloudAuths {
				a := y.CloudAuths[i]
				if _, ok := nameMatches[i]; ok {
					deleted = a
				} else {
					keep = append(keep, a)
				}
			}
			y.CloudAuths = keep

			// Gather all profiles containing this auth and
			// prompt confirm if the user is ok with deleting
			// auth attached to profiles.
			attachedProfiles := make(map[int]struct{})
			for i, p := range y.Profiles {
				if p.CloudCluster.HasAuth(deleted) {
					attachedProfiles[i] = struct{}{}
				}
			}

			if len(attachedProfiles) > 0 {
				names := make([]string, 0, len(attachedProfiles))
				for i := range attachedProfiles {
					names = append(names, y.Profiles[i].Name)
				}
				sort.Strings(names)
				fmt.Println("The following profiles are currently using this cloud auth:")
				for _, name := range names {
					fmt.Printf("  %s\n", name)
				}
				fmt.Println("Deleting this auth will mean the profiles no longer can talk to the cloud.")
				fmt.Println("The profiles will only be able to talk to the cluster via SASL credentials,")
				fmt.Println("which you must set in the profile manually via 'rpk profile edit'.")
				c, err := out.Confirm("Do you still want to delete this auth and remove it from the affected profiles?")
				out.MaybeDie(err, "unable to confirm deletion: %v", err)
				if !c {
					fmt.Println("Deletion canceled.")
					return
				}
				for i := range attachedProfiles {
					p := &y.Profiles[i]
					p.FromCloud = false
					p.CloudCluster.AuthOrgID = ""
					p.CloudCluster.AuthKind = ""
				}
			}

			err = y.Write(fs)
			out.MaybeDie(err, "unable to write rpk.yaml: %v", err)

			wasUsing := y.CurrentCloudAuthOrgID == deleted.OrgID && y.CurrentCloudAuthKind == deleted.Kind

			fmt.Printf("Deleted cloud auth %q.\n", name)
			if wasUsing {
				fmt.Print(`This was the current auth selected.

You may need to reauthenticate with 'rpk cloud login' or swap to a different
profile that uses different auth using cloud API commands again.
`)
			}
		},
	}
	return cmd
}
