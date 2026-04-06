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
	"strings"

	dataplanev1 "buf.build/gen/go/redpandadata/dataplane/protocolbuffers/go/redpanda/api/dataplane/v1"
	"github.com/redpanda-data/common-go/rpadmin"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

const (
	rolePrefix  = "RedpandaRole:"
	userPrefix  = "User:"
	groupPrefix = "Group:"
)

func NewCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "role",
		Aliases: []string{"access", "roles"},
		Args:    cobra.ExactArgs(0),
		Short:   "Manage Redpanda roles",
	}
	cmd.AddCommand(
		assignCommand(fs, p),
		createCommand(fs, p),
		deleteCommand(fs, p),
		describeCommand(fs, p),
		listCommand(fs, p),
		unassignCommand(fs, p),
	)
	p.InstallAdminFlags(cmd)
	p.InstallFormatFlag(cmd)
	return cmd
}

// parsePrincipal returns the prefix, and principal. If no prefix is present,
// returns 'User'.
func parsePrincipal(p string) (principalType string, name string) {
	if s, ok := strings.CutPrefix(p, userPrefix); ok {
		return "User", s
	}
	if s, ok := strings.CutPrefix(p, groupPrefix); ok {
		return "Group", s
	}

	return "User", p
}

// parseRoleMembers parses a --principal flag to a []adminapi.RoleMember.
func parseRoleMember(principals []string) []rpadmin.RoleMember {
	var members []rpadmin.RoleMember
	for _, p := range principals {
		pType, name := parsePrincipal(p)
		members = append(members, rpadmin.RoleMember{Name: name, PrincipalType: pType})
	}
	return members
}

// roleMemberToMembership converts []rpadmin.RoleMember to []*dataplanev1.RoleMembership.
func roleMemberToMembership(members []rpadmin.RoleMember) []*dataplanev1.RoleMembership {
	result := make([]*dataplanev1.RoleMembership, len(members))
	for i, m := range members {
		result[i] = &dataplanev1.RoleMembership{Principal: m.PrincipalType + ":" + m.Name}
	}
	return result
}

// membershipToRoleMember converts []*dataplanev1.RoleMembership to []rpadmin.RoleMember.
// If memberships is nil, returns an empty slice (to avoid printing "null" in
// JSON/YAML output).
func membershipToRoleMember(memberships []*dataplanev1.RoleMembership) []rpadmin.RoleMember {
	result := make([]rpadmin.RoleMember, len(memberships))
	for i, m := range memberships {
		pType, name := parsePrincipal(m.Principal)
		result[i] = rpadmin.RoleMember{Name: name, PrincipalType: pType}
	}
	return result
}
