// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package acl

import (
	"fmt"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/kafka"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/schemaregistry"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/types"
)

func newCreateCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var a acls
	cmd := &cobra.Command{
		Use:   "create",
		Short: "Create ACLs",
		Long: `Create ACLs.

See the 'rpk security acl' help text for a full write up on ACLs. Following the
multiplying effect of combining flags, the create command works on a
straightforward basis: every ACL combination is a created ACL.

As mentioned in the 'rpk security acl' help text, if no host is specified, an
allowed principal is allowed access from all hosts. The wildcard principal '*'
allows all principals. At least one principal, one host, one resource, and one
operation is required to create a single ACL.
`,
		Example: `
Allow all permissions to user bar on topic "foo" and group "g":
  rpk security acl create --allow-principal bar --operation all --topic foo --group g

Allow all permissions to role bar on topic "foo" and group "g":
  rpk security acl create --allow-role bar --operation all --topic foo --group g

Allow read permissions to all users on topics biz and baz:
  rpk security acl create --allow-principal '*' --operation read --topic biz,baz

Allow write permissions to user buzz to transactional ID "txn":
  rpk security acl create  --allow-principal User:buzz --operation write --transactional-id txn

Allow read permissions to user panda on topic "bar" and schema registry subject "bar-value":
  rpk security acl create --allow-principal panda --operation read --topic bar --registry-subject bar-value
`,
		Args: cobra.NoArgs,
		Run: func(cmd *cobra.Command, _ []string) {
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)

			adm, err := kafka.NewAdmin(fs, p)
			out.MaybeDie(err, "unable to initialize kafka client: %v", err)
			defer adm.Close()

			srClient, err := schemaregistry.NewClient(fs, p)
			out.MaybeDie(err, "unable to initialize schema registry client: %v", err)

			kCreations, srCreations, err := a.createCreations()
			out.MaybeDieErr(err)

			var results []aclWithMessage
			if kCreations != nil {
				kResults, err := adm.CreateACLs(cmd.Context(), kCreations)
				out.MaybeDie(err, "unable to create ACLs: %v", err)
				for _, c := range kResults {
					errMsg := kafka.ErrMessage(c.Err)
					if c.ErrMessage != "" {
						errMsg = fmt.Sprintf("%v: %v", errMsg, c.ErrMessage)
					}
					results = append(results, aclWithMessage{
						c.Principal,
						c.Host,
						c.Type.String(),
						c.Name,
						c.Pattern.String(),
						c.Operation.String(),
						c.Permission.String(),
						errMsg,
					})
				}
			}
			if len(srCreations) > 0 {
				err = srClient.CreateACLs(cmd.Context(), srCreations)
				errMsg := ""
				if err != nil {
					errMsg = err.Error()
				}
				for _, c := range srCreations {
					results = append(results, aclWithMessage{
						c.Principal,
						c.Host,
						string(c.ResourceType),
						c.Resource,
						string(c.PatternType),
						string(c.Operation),
						string(c.Permission),
						errMsg,
					})
				}
			}

			if len(results) == 0 {
				fmt.Println("Specified flags created no ACLs.")
				return
			}
			types.Sort(results)

			tw := out.NewTable(headersWithError...)
			defer tw.Flush()
			for _, r := range results {
				tw.PrintStructFields(r)
			}
		},
	}
	p.InstallKafkaFlags(cmd)
	a.addCreateFlags(cmd)
	return cmd
}

func (a *acls) addCreateFlags(cmd *cobra.Command) {
	a.addDeprecatedFlags(cmd)

	cmd.Flags().StringSliceVar(&a.topics, topicFlag, nil, "Topic to grant ACLs for (repeatable)")
	cmd.Flags().StringSliceVar(&a.groups, groupFlag, nil, "Group to grant ACLs for (repeatable)")
	cmd.Flags().BoolVar(&a.cluster, clusterFlag, false, "Whether to grant ACLs to the cluster")
	cmd.Flags().StringSliceVar(&a.txnIDs, txnIDFlag, nil, "Transactional IDs to grant ACLs for (repeatable)")
	cmd.Flags().BoolVar(&a.registry, registryFlag, false, "Whether to grant ACLs for the schema registry")
	cmd.Flags().StringSliceVar(&a.subjects, subjectFlag, nil, "Schema Registry subjects to grant ACLs for (repeatable)")

	cmd.Flags().StringVar(&a.resourcePatternType, patternFlag, "literal", "Pattern to use when matching resource names (literal or prefixed)")
	cmd.Flags().StringSliceVar(&a.operations, operationFlag, nil, "Operation to grant (repeatable)")

	cmd.Flags().StringSliceVar(&a.allowPrincipals, allowPrincipalFlag, nil, "Principals for which these permissions will be granted (repeatable)")
	cmd.Flags().StringSliceVar(&a.allowRoles, allowRoleFlag, nil, "Roles for which these permissions will be granted (repeatable)")
	cmd.Flags().StringSliceVar(&a.allowHosts, allowHostFlag, nil, "Hosts from which access will be granted (repeatable)")
	cmd.Flags().StringSliceVar(&a.denyPrincipals, denyPrincipalFlag, nil, "Principal for which these permissions will be denied (repeatable)")
	cmd.Flags().StringSliceVar(&a.denyRoles, denyRoleFlag, nil, "Role for which these permissions will be denied (repeatable)")
	cmd.Flags().StringSliceVar(&a.denyHosts, denyHostFlag, nil, "Hosts from from access will be denied (repeatable)")
}
