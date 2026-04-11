// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package group

import (
	"testing"

	adminv2 "buf.build/gen/go/redpandadata/core/protocolbuffers/go/redpanda/core/admin/v2"
	"github.com/stretchr/testify/require"
)

var (
	engGroup = &adminv2.RoleMember{Member: &adminv2.RoleMember_Group{Group: &adminv2.RoleGroup{Name: "engineering"}}}
	opsGroup = &adminv2.RoleMember{Member: &adminv2.RoleMember_Group{Group: &adminv2.RoleGroup{Name: "ops"}}}
)

func TestCollectGroupsFromAdminRoles(t *testing.T) {
	var uninit []string

	for _, tc := range []struct {
		name  string
		roles []*adminv2.Role
		want  []string
	}{
		{
			name:  "no roles",
			roles: nil,
			want:  uninit,
		},
		{
			name: "roles with no members",
			roles: []*adminv2.Role{
				{Name: "admin"},
			},
			want: uninit,
		},
		{
			name: "role with user member only",
			roles: []*adminv2.Role{
				{
					Name: "admin",
					Members: []*adminv2.RoleMember{
						{Member: &adminv2.RoleMember_User{User: &adminv2.RoleUser{Name: "alice"}}},
					},
				},
			},
			want: uninit,
		},
		{
			name: "role with group member",
			roles: []*adminv2.Role{
				{
					Name:    "admin",
					Members: []*adminv2.RoleMember{engGroup},
				},
			},
			want: []string{"engineering"},
		},
		{
			name: "multiple roles with group members, deduplication and sorting",
			roles: []*adminv2.Role{
				{
					Name:    "admin",
					Members: []*adminv2.RoleMember{engGroup},
				},
				{
					Name:    "reader",
					Members: []*adminv2.RoleMember{engGroup, opsGroup},
				},
			},
			want: []string{"engineering", "ops"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := collectGroupsFromAdminRoles(tc.roles)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestRolesForGroup(t *testing.T) {
	for _, tc := range []struct {
		name      string
		roles     []*adminv2.Role
		groupName string
		want      []string
	}{
		{
			name:      "no roles",
			roles:     nil,
			groupName: "engineering",
			want:      nil,
		},
		{
			name: "group not in any role",
			roles: []*adminv2.Role{
				{
					Name:    "admin",
					Members: []*adminv2.RoleMember{opsGroup},
				},
			},
			groupName: "engineering",
			want:      nil,
		},
		{
			name: "group in one role",
			roles: []*adminv2.Role{
				{
					Name:    "admin",
					Members: []*adminv2.RoleMember{engGroup},
				},
				{
					Name:    "reader",
					Members: []*adminv2.RoleMember{opsGroup},
				},
			},
			groupName: "engineering",
			want:      []string{"admin"},
		},
		{
			name: "group in multiple roles, sorted",
			roles: []*adminv2.Role{
				{
					Name:    "writer",
					Members: []*adminv2.RoleMember{engGroup},
				},
				{
					Name:    "admin",
					Members: []*adminv2.RoleMember{engGroup},
				},
			},
			groupName: "engineering",
			want:      []string{"admin", "writer"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := rolesForGroup(tc.roles, tc.groupName)
			require.Equal(t, tc.want, got)
		})
	}
}
