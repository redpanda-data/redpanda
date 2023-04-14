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
	"context"
	"fmt"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/kafka"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
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

See the 'rpk acl' help text for a full write up on ACLs. Following the
multiplying effect of combining flags, the create command works on a
straightforward basis: every ACL combination is a created ACL.

As mentioned in the 'rpk acl' help text, if no host is specified, an allowed
principal is allowed access from all hosts. The wildcard principal '*' allows
all principals. At least one principal, one host, one resource, and one
operation is required to create a single ACL.

Allow all permissions to user bar on topic "foo" and group "g":
    --allow-principal bar --operation all --topic foo --group g
Allow read permissions to all users on topics biz and baz:
    --allow-principal '*' --operation read --topic biz,baz
Allow write permissions to user buzz to transactional id "txn":
    --allow-principal User:buzz --operation write --transactional-id txn
`,

		Args: cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, _ []string) {
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			adm, err := kafka.NewAdmin(fs, p, cfg)
			out.MaybeDie(err, "unable to initialize kafka client: %v", err)
			defer adm.Close()

			b, err := a.createCreations()
			out.MaybeDieErr(err)
			results, err := adm.CreateACLs(context.Background(), b)
			out.MaybeDie(err, "unable to create ACLs: %v", err)
			if len(results) == 0 {
				fmt.Println("Specified flags created no ACLs.")
				return
			}
			types.Sort(results)

			tw := out.NewTable(headersWithError...)
			defer tw.Flush()
			for _, c := range results {
				tw.PrintStructFields(aclWithMessage{
					c.Principal,
					c.Host,
					c.Type,
					c.Name,
					c.Pattern,
					c.Operation,
					c.Permission,
					kafka.ErrMessage(c.Err),
				})
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

	cmd.Flags().StringVar(&a.resourcePatternType, patternFlag, "literal", "Pattern to use when matching resource names (literal or prefixed)")

	cmd.Flags().StringSliceVar(&a.operations, operationFlag, nil, "Operation to grant (repeatable)")

	cmd.Flags().StringSliceVar(&a.allowPrincipals, allowPrincipalFlag, nil, "Principals for which these permissions will be granted (repeatable)")
	cmd.Flags().StringSliceVar(&a.allowHosts, allowHostFlag, nil, "Hosts from which access will be granted (repeatable)")
	cmd.Flags().StringSliceVar(&a.denyPrincipals, denyPrincipalFlag, nil, "Principal for which these permissions will be denied (repeatable)")
	cmd.Flags().StringSliceVar(&a.denyHosts, denyHostFlag, nil, "Hosts from from access will be denied (repeatable)")
}
