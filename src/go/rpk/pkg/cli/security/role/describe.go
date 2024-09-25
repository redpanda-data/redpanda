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
	"context"
	"fmt"

	"github.com/redpanda-data/common-go/rpadmin"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/kafka"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/types"
)

type roleACL struct {
	Principal           string `json:"principal,omitempty" yaml:"principal,omitempty"`
	Host                string `json:"host,omitempty" yaml:"host,omitempty"`
	ResourceType        string `json:"resource_type,omitempty" yaml:"resource_type,omitempty"`
	ResourceName        string `json:"resource_name,omitempty" yaml:"resource_name,omitempty"`
	ResourcePatternType string `json:"resource_pattern_type,omitempty" yaml:"resource_pattern_type,omitempty"`
	Operation           string `json:"operation,omitempty" yaml:"operation,omitempty"`
	Permission          string `json:"permission,omitempty" yaml:"permission,omitempty"`
}

type describeResponse struct {
	Permissions []roleACL            `json:"permissions" yaml:"permissions"`
	Members     []rpadmin.RoleMember `json:"members" yaml:"members"`
}

func describeCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var (
		all         bool
		permissions bool
		members     bool
	)
	cmd := &cobra.Command{
		Use:     "describe [ROLE]",
		Aliases: []string{"info"},
		Short:   "Describe a Redpanda role",
		Long: `Describe a Redpanda role.

This command describes a role, including the ACLs associated to the role, and 
lists members who are assigned the role.
`,
		Example: `
Describe the role 'red' (print members and ACLs)
  rpk security role describe red

Print only the members of role 'red'
  rpk security role describe red --print-members

Print only the ACL associated to the role 'red'
  rpk security role describe red --print-permissions
`,
		Args: cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			f := p.Formatter
			if h, ok := f.Help(describeResponse{}); ok {
				out.Exit(h)
			}
			if (!permissions && !members) || all {
				permissions, members = true, true
			}
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			config.CheckExitServerlessAdmin(p)

			cl, err := adminapi.NewClient(cmd.Context(), fs, p)
			out.MaybeDie(err, "unable to initialize admin api client: %v", err)

			adm, err := kafka.NewAdmin(fs, p)
			out.MaybeDie(err, "unable to initialize kafka client: %v", err)
			defer adm.Close()

			roleName := args[0]
			err = describeAndPrintRole(cmd.Context(), cl, adm, f, roleName, permissions, members)
			out.MaybeDieErr(err)
		},
	}

	cmd.Flags().BoolVarP(&permissions, "print-permissions", "p", false, "Print the role permissions section")
	cmd.Flags().BoolVarP(&members, "print-members", "m", false, "Print the members section")
	cmd.Flags().BoolVarP(&all, "print-all", "a", false, "Print all sections")
	return cmd
}

func describeAndPrintRole(ctx context.Context, admCl *rpadmin.AdminAPI, kafkaAdmCl *kadm.Client, f config.OutFormatter, roleName string, permissions, principals bool) error {
	// Get ACLs that belong to RedpandaRole:<roleName>
	principal := rolePrefix + roleName
	b := kadm.NewACLs().
		AnyResource().
		Allow(principal).AllowHosts().
		Deny(principal).DenyHosts().
		Operations(kadm.OpAny).
		ResourcePatternType(kadm.ACLPatternAny)

	results, err := kafkaAdmCl.DescribeACLs(ctx, b)
	if err != nil {
		return fmt.Errorf("unable to list ACLs: %v", err)
	}
	types.Sort(results)

	// Get Role Members
	r, err := admCl.RoleMembers(ctx, roleName)
	if err != nil {
		return fmt.Errorf("unable to retrieve role members of role %q: %v", roleName, err)
	}
	// Do this to avoid printing `null` in --format json
	members := []rpadmin.RoleMember{}
	if r.Members != nil {
		members = r.Members
	}
	described := describeResponse{
		Members:     members,
		Permissions: describedToRoleACL(results),
	}

	// Print according to format
	if isText, _, s, err := f.Format(described); !isText {
		if err != nil {
			return fmt.Errorf("unable to print in the required format %q: %v", f.Kind, err)
		}
		fmt.Println(s)
		return nil
	}
	var (
		secPermissions = "permissions"
		secPrincipals  = fmt.Sprintf("principals (%v)", len(r.Members))
	)
	sections := out.NewMaybeHeaderSections(
		out.ConditionalSectionHeaders(map[string]bool{
			secPermissions: permissions,
			secPrincipals:  principals,
		})...,
	)

	sections.Add(secPermissions, func() {
		tw := out.NewTable("Principal",
			"Host",
			"Resource-Type",
			"Resource-Name",
			"Resource-Pattern-Type",
			"Operation",
			"Permission",
			"Error",
		)
		defer tw.Flush()
		for _, p := range described.Permissions {
			tw.PrintStructFields(p)
		}
	})

	sections.Add(secPrincipals, func() {
		tw := out.NewTable("NAME", "TYPE")
		defer tw.Flush()
		for _, r := range r.Members {
			tw.PrintStructFields(r)
		}
	})

	return nil
}

func describedToRoleACL(results kadm.DescribeACLsResults) []roleACL {
	ret := []roleACL{}
	for _, f := range results {
		for _, d := range f.Described {
			ret = append(ret, roleACL{
				Principal:           d.Principal,
				Host:                d.Host,
				ResourceType:        d.Type.String(),
				ResourceName:        d.Name,
				ResourcePatternType: d.Pattern.String(),
				Operation:           d.Operation.String(),
				Permission:          d.Permission.String(),
			})
		}
	}
	return ret
}
