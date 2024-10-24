// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package security

import (
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/security/acl"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/security/role"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/security/user"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func NewCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "security",
		Aliases: []string{"sec"},
		Args:    cobra.ExactArgs(0),
		Short:   "Manage Redpanda security",
	}
	cmd.AddCommand(
		acl.NewCommand(fs, p),
		role.NewCommand(fs, p),
		user.NewCommand(fs, p),
	)
	return cmd
}
