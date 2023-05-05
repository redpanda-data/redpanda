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

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newDeleteCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	return &cobra.Command{
		Use:               "delete [NAME]",
		Short:             "Delete an rpk cloud auth",
		Args:              cobra.ExactArgs(1),
		ValidArgsFunction: validAuths(fs, p),
		Run: func(_ *cobra.Command, args []string) {
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			y, ok := cfg.ActualRpkYaml()
			if !ok {
				out.Die("rpk.yaml file does not exist")
			}

			name := args[0]
			wasUsing, err := deleteAuth(fs, y, name)
			out.MaybeDieErr(err)
			fmt.Printf("Deleted cloud auth %q.\n", name)
			if wasUsing {
				fmt.Println("The current context was using this cloud auth.\nYou may need to reauthenticate before the current context can be used again.")
			}
		},
	}
}

func deleteAuth(
	fs afero.Fs,
	y *config.RpkYaml,
	name string,
) (wasUsing bool, err error) {
	idx := -1
	for i, a := range y.CloudAuths {
		if a.Name == name {
			idx = i
			break
		}
	}
	if idx == -1 {
		return false, fmt.Errorf("cloud auth %q does not exist", name)
	}
	y.CloudAuths = append(y.CloudAuths[:idx], y.CloudAuths[idx+1:]...)
	ca := y.Auth(y.CurrentCloudAuth)
	if wasUsing = ca != nil && ca.Name == name; wasUsing {
		y.CurrentCloudAuth = ""
	}
	if err := y.Write(fs); err != nil {
		return false, fmt.Errorf("unable to write rpk file: %v", err)
	}
	return wasUsing, nil
}
