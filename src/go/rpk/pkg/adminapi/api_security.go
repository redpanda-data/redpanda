// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package adminapi

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
)

const (
	baseSecurityEndpoint = "/v1/security/"
	baseRoleEndpoint     = baseSecurityEndpoint + "roles"
)

// Role is a representation of a Role as returned by the Admin API.
type Role struct {
	Name string `json:"name" yaml:"name"`
}

// RoleMember is a representation of a principal.
type RoleMember struct {
	Name          string `json:"name" yaml:"name"`
	PrincipalType string `json:"principal_type" yaml:"principal_type"`
}

// RolesResponse represent the response from Roles method.
type RolesResponse struct {
	Roles []Role `json:"roles" yaml:"roles"`
}

// CreateRole is both the request and response from the CreateRole method.
type CreateRole struct {
	RoleName string `json:"role" yaml:"role"`
}

// PatchRoleResponse is the response of the PatchRole method.
type PatchRoleResponse struct {
	RoleName string       `json:"role" yaml:"role"`
	Added    []RoleMember `json:"added" yaml:"added"`
	Removed  []RoleMember `json:"removed" yaml:"removed"`
}

type patchRoleRequest struct {
	Add    []RoleMember `json:"add,omitempty"`
	Remove []RoleMember `json:"remove,omitempty"`
}

// RoleMemberResponse is the response of the RoleMembers method.
type RoleMemberResponse struct {
	Members []RoleMember `json:"members" yaml:"members"`
}

// Roles returns the roles in Redpanda, use 'prefix', 'principal', and
// 'principalType' to filter the results. principalType must be set along with
// principal. It has no effect on its own.
func (a *AdminAPI) Roles(ctx context.Context, prefix, principal, principalType string) (RolesResponse, error) {
	var roles RolesResponse
	u, qs := baseRoleEndpoint, url.Values{}
	if prefix != "" {
		qs.Add("filter", prefix)
	}
	if principal != "" {
		if principalType == "" {
			return RolesResponse{}, errors.New("principalType can not be empty if principal is set")
		}
		qs.Add("principal", principal)
		qs.Add("principal_type", principalType)
	}
	if queryString := qs.Encode(); queryString != "" {
		u += "?" + queryString
	}
	return roles, a.sendAny(ctx, http.MethodGet, u, nil, &roles)
}

// CreateRole creates a Role in Redpanda with the given name.
func (a *AdminAPI) CreateRole(ctx context.Context, name string) (CreateRole, error) {
	var res CreateRole
	return res, a.sendAny(ctx, http.MethodPost, baseRoleEndpoint, CreateRole{name}, &res)
}

// DeleteRole deletes a Role in Redpanda with the given name. If deleteACL is
// true, Redpanda will delete ACLs bound to the role.
func (a *AdminAPI) DeleteRole(ctx context.Context, name string, deleteACL bool) error {
	return a.sendAny(
		ctx,
		http.MethodDelete,
		fmt.Sprintf("%v/%v?delete_acls=%v", baseRoleEndpoint, name, deleteACL),
		nil,
		nil,
	)
}

// AssignRole assign the role 'roleName' to the passed members.
func (a *AdminAPI) AssignRole(ctx context.Context, roleName string, add []RoleMember) (PatchRoleResponse, error) {
	var res PatchRoleResponse
	body := patchRoleRequest{
		Add: add,
	}
	return res, a.sendAny(ctx, http.MethodPost, fmt.Sprintf("%v/%v/members", baseRoleEndpoint, roleName), body, &res)
}

// UnassignRole unassigns the role 'roleName' from the passed members.
func (a *AdminAPI) UnassignRole(ctx context.Context, roleName string, remove []RoleMember) (PatchRoleResponse, error) {
	var res PatchRoleResponse
	body := patchRoleRequest{
		Remove: remove,
	}
	return res, a.sendAny(ctx, http.MethodPost, fmt.Sprintf("%v/%v/members", baseRoleEndpoint, roleName), body, &res)
}

// UpdateRoleMembership updates the role membership for 'roleName' adding and removing the passed members.
func (a *AdminAPI) UpdateRoleMembership(ctx context.Context, roleName string, add, remove []RoleMember) (PatchRoleResponse, error) {
	var res PatchRoleResponse
	body := patchRoleRequest{
		Add:    add,
		Remove: remove,
	}
	return res, a.sendAny(ctx, http.MethodPost, fmt.Sprintf("%v/%v/members", baseRoleEndpoint, roleName), body, &res)
}

// RoleMembers returns the list of RoleMembers of a given role.
func (a *AdminAPI) RoleMembers(ctx context.Context, roleName string) (RoleMemberResponse, error) {
	var res RoleMemberResponse
	return res, a.sendAny(ctx, http.MethodGet, fmt.Sprintf("%v/%v/members", baseRoleEndpoint, roleName), nil, &res)
}
