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
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func NewCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:        "namespace",
		Aliases:    []string{"ns"},
		SuggestFor: []string{"namespaces"},
		Args:       cobra.ExactArgs(0),
		Short:      "Interact with Namespaces in Redpanda Cloud",
	}
	cmd.AddCommand(
		createCommand(fs, p),
		deleteCommand(fs, p),
		listCommand(fs, p),
	)
	p.InstallCloudFlags(cmd)
	p.InstallFormatFlag(cmd)
	return cmd
}
