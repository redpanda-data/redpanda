// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package user

import (
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func NewCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "user",
		Short: "Manage SASL users",
		Long: `Manage SASL users.

If SASL is enabled, a SASL user is what you use to talk to Redpanda, and ACLs
control what your user has access to. See 'rpk security  acl --help' for more
information about ACLs, and 'rpk security acl user create --help' for more
information about creating SASL users. Using SASL requires setting "enable_sasl:
true" in the redpanda section of your redpanda.yaml.
`,
	}
	p.InstallAdminFlags(cmd)
	p.InstallKafkaFlags(cmd) // old ACL user commands have this, and Kafka SASL creds are used for admin API basic auth
	p.InstallFormatFlag(cmd)
	cmd.AddCommand(
		newCreateUserCommand(fs, p),
		newDeleteUserCommand(fs, p),
		newListUsersCommand(fs, p),
		newUpdateCommand(fs, p),
	)
	return cmd
}
