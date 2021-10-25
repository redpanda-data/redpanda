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
	"fmt"

	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/config"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/kafka"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/out"
)

func NewDeleteCommand(fs afero.Fs) *cobra.Command {
	var (
		a               acls
		printAllFilters bool
		dry             bool
	)
	cmd := &cobra.Command{
		Use:   "delete",
		Short: "Delete ACLs",
		Args:  cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, _ []string) {
			p := config.ParamsFromCommand(cmd)
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			cl, err := kafka.NewFranzClient(fs, p, cfg)
			out.MaybeDie(err, "unable to initialize kafka client: %v", err)
			defer cl.Close()

			deletions, describes, err := a.createDeletionsAndDescribes(false)
			out.MaybeDieErr(err)
			if len(deletions) == 0 {
				out.Exit("Specified flags result in no ACL filters for deleting.")
			}
			if dry {
				describeReqResp(cl, printAllFilters, describes)
				return
			}
			deleteReqResp(cl, printAllFilters, deletions)
		},
	}
	a.addDeleteFlags(cmd)
	cmd.Flags().BoolVarP(&printAllFilters, "print-filters", "f", false, "print the filters that were requested (failed filters are always printed)")
	cmd.Flags().BoolVarP(&dry, "dry", "d", false, "dry run: validate what would be deleted (alias for list)")
	return cmd
}

func (a *acls) addDeleteFlags(cmd *cobra.Command) {
	a.addDeprecatedFlags(cmd)

	cmd.Flags().StringArrayVar(&a.topics, topicFlag, nil, "topic to remove ACLs for (repeatable)")
	cmd.Flags().StringArrayVar(&a.groups, groupFlag, nil, "group to remove ACLs for (repeatable)")
	cmd.Flags().BoolVar(&a.cluster, clusterFlag, false, "whether to remove ACLs to the cluster")
	cmd.Flags().StringArrayVar(&a.txnIDs, txnIDFlag, nil, "transactional IDs to remove ACLs for (repeatable)")

	cmd.Flags().StringVar(&a.resourcePatternType, patternFlag, "literal", "pattern to use when matching resource names (any, match, literal, or prefixed)")

	cmd.Flags().StringSliceVar(&a.operations, operationFlag, nil, "operation to remove (repeatable)")

	cmd.Flags().StringSliceVar(&a.allowPrincipals, allowPrincipalFlag, nil, "allowed principal ACLs to remove (repeatable)")
	cmd.Flags().StringSliceVar(&a.allowHosts, allowHostFlag, nil, "allowed host ACLs to remove (repeatable)")
	cmd.Flags().StringSliceVar(&a.denyPrincipals, denyPrincipalFlag, nil, "denied principal ACLs to remove (repeatable)")
	cmd.Flags().StringSliceVar(&a.denyHosts, denyHostFlag, nil, "denied host ACLs to remove (repeatable)")

	cmd.Flags().StringSliceVar(&a.any, anyFlag, nil, "flags to opt into matching anything for (e.g., --any=topic,group,allow-host or --any=all)")
}

func deleteReqResp(
	cl *kgo.Client,
	printAllFilters bool,
	deletions []kmsg.DeleteACLsRequestFilter,
) {
	req := kmsg.NewPtrDeleteACLsRequest()
	req.Filters = deletions
	resp, err := req.RequestWith(context.Background(), cl)
	out.MaybeDie(err, "unable to issue ACL deletion request: %v", err)
	if len(resp.Results) != len(deletions) {
		out.Die("received %d filter results to %d filter requests", len(resp.Results), len(deletions))
	}

	// If any filters failed, or if all filters are requested, we print the
	// filter section.
	var printFailedFilters bool
	for _, r := range resp.Results {
		if r.ErrorCode != 0 {
			printFailedFilters = true
			break
		}
	}
	if printAllFilters || printFailedFilters {
		out.Section("filters")
		fmt.Println()
		printDeleteFilters(printAllFilters, req, resp)
		fmt.Println("matches")
	}

	printDeleteResults(deletions, resp.Results)
}

func printDeleteFilters(
	all bool, req *kmsg.DeleteACLsRequest, resp *kmsg.DeleteACLsResponse,
) {
	tw := out.NewTable(headersWithError...)
	defer tw.Flush()
	for i, r := range resp.Results {
		f := req.Filters[i]
		if r.ErrorCode == 0 && !all {
			continue
		}
		tw.PrintStructFields(aclWithMessage{
			unptr(f.Principal),
			unptr(f.Host),
			f.ResourceType,
			unptr(f.ResourceName),
			f.ResourcePatternType,
			f.Operation,
			f.PermissionType,
			kafka.MaybeErrMessage(r.ErrorCode),
		})
	}
}

func printDeleteResults(
	deletions []kmsg.DeleteACLsRequestFilter,
	results []kmsg.DeleteACLsResponseResult,
) {
	tw := out.NewTable(headersWithError...)
	defer tw.Flush()
	for i, r := range results {
		if err := kerr.TypedErrorForCode(r.ErrorCode); err != nil {
			d := deletions[i]
			var principal, host, resource string
			if d.Principal != nil {
				principal = *d.Principal
			}
			if d.Host != nil {
				host = *d.Host
			}
			if d.ResourceName != nil {
				resource = *d.ResourceName
			}
			tw.PrintStructFields(aclWithMessage{
				principal,
				host,
				d.ResourceType,
				resource,
				d.ResourcePatternType,
				d.Operation,
				d.PermissionType,
				err.Message,
			})
			continue
		}
		for _, m := range r.MatchingACLs {
			tw.PrintStructFields(aclWithMessage{
				m.Principal,
				m.Host,
				m.ResourceType,
				m.ResourceName,
				m.ResourcePatternType,
				m.Operation,
				m.PermissionType,
				kafka.MaybeErrMessage(m.ErrorCode),
			})
		}
	}
}
