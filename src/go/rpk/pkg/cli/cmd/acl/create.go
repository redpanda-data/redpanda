// Copyright 2021 Vectorized, Inc.
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

	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/config"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/kafka"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/out"
)

func NewCreateCommand(fs afero.Fs) *cobra.Command {
	var a acls
	cmd := &cobra.Command{
		Use:   "create",
		Short: "Create ACLs.",

		Args: cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, _ []string) {
			p := config.ParamsFromCommand(cmd)
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			cl, err := kafka.NewFranzClient(fs, p, cfg)
			out.MaybeDie(err, "unable to initialize kafka client: %v", err)
			defer cl.Close()

			creations, err := a.createCreations()
			out.MaybeDieErr(err)
			if len(creations) == 0 {
				out.Exit("Specified flags result in no ACLs to be created.")
			}

			req := kmsg.NewPtrCreateACLsRequest()
			req.Creations = creations
			resp, err := req.RequestWith(context.Background(), cl)
			out.MaybeDie(err, "unable to issue ACL creation request: %v", err)
			if len(resp.Results) != len(req.Creations) {
				out.Die("response contained only %d results out of the expected %d", len(resp.Results), len(req.Creations))
			}

			tw := out.NewTable(headersWithError...)
			defer tw.Flush()
			for i, r := range resp.Results {
				c := req.Creations[i]
				tw.PrintStructFields(aclWithMessage{
					c.Principal,
					c.Host,
					c.ResourceType,
					c.ResourceName,
					c.ResourcePatternType,
					c.Operation,
					c.PermissionType,
					kafka.MaybeErrMessage(r.ErrorCode),
				})
			}
		},
	}
	a.addCreateFlags(cmd)
	return cmd
}

func (a *acls) addCreateFlags(cmd *cobra.Command) {
	a.addDeprecatedFlags(cmd)

	cmd.Flags().StringArrayVar(&a.topics, topicFlag, nil, "topic to grant ACLs for (repeatable)")
	cmd.Flags().StringArrayVar(&a.groups, groupFlag, nil, "group to grant ACLs for (repeatable)")
	cmd.Flags().BoolVar(&a.cluster, clusterFlag, false, "whether to grant ACLs to the cluster")
	cmd.Flags().StringArrayVar(&a.txnIDs, txnIDFlag, nil, "transactional IDs to grant ACLs for (repeatable)")

	cmd.Flags().StringVar(&a.resourcePatternType, patternFlag, "literal", "pattern to use when matching resource names (literal or prefixed)")

	cmd.Flags().StringSliceVar(&a.operations, operationFlag, nil, "operation to grant (repeatable)")

	cmd.Flags().StringSliceVar(&a.allowPrincipals, allowPrincipalFlag, nil, "principals for which these permissions will be granted (repeatable)")
	cmd.Flags().StringSliceVar(&a.allowHosts, allowHostFlag, nil, "hosts from which access will be granted (repeatable)")
	cmd.Flags().StringSliceVar(&a.denyPrincipals, denyPrincipalFlag, nil, "principal for which these permissions will be denied (repeatable)")
	cmd.Flags().StringSliceVar(&a.denyHosts, denyHostFlag, nil, "hosts from from access will be denied (repeatable)")
}
