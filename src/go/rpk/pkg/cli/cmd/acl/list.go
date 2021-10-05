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
	"sync"

	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/types"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/config"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/kafka"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/out"
)

func NewListCommand(fs afero.Fs) *cobra.Command {
	var a acls
	var printAllFilters bool
	cmd := &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls", "describe"},
		Short:   "List ACLs",
		Args:    cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, _ []string) {
			p := config.ParamsFromCommand(cmd)
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			cl, err := kafka.NewFranzClient(fs, p, cfg)
			out.MaybeDie(err, "unable to initialize kafka client: %v", err)
			defer cl.Close()

			_, describes, err := a.createDeletionsAndDescribes(true)
			out.MaybeDieErr(err)
			if len(describes) == 0 {
				out.Exit("Specified flags result in no ACL filters for listing.")
			}
			describeReqResp(cl, printAllFilters, describes)
		},
	}
	a.addListFlags(cmd)
	cmd.Flags().BoolVarP(&printAllFilters, "print-filters", "f", false, "print the filters that were requested (failed filters are always printed)")
	return cmd
}

func (a *acls) addListFlags(cmd *cobra.Command) {
	a.addDeprecatedFlags(cmd)

	// List has a few more extra deprecated flags.
	cmd.Flags().StringSliceVar(&a.listPermissions, "permission", nil, "")
	cmd.Flags().StringSliceVar(&a.listPrincipals, "principal", nil, "")
	cmd.Flags().StringSliceVar(&a.listHosts, "host", nil, "")
	cmd.Flags().MarkDeprecated("permission", "use --{allow,deny}-{host,principal}")
	cmd.Flags().MarkDeprecated("principal", "use --{allow,deny}-{host,principal}")
	cmd.Flags().MarkDeprecated("host", "use --{allow,deny}-{host,principal}")

	cmd.Flags().StringArrayVar(&a.topics, topicFlag, nil, "topic to match ACLs for (repeatable)")
	cmd.Flags().StringArrayVar(&a.groups, groupFlag, nil, "group to match ACLs for (repeatable)")
	cmd.Flags().BoolVar(&a.cluster, clusterFlag, false, "whether to match ACLs to the cluster")
	cmd.Flags().StringArrayVar(&a.txnIDs, txnIDFlag, nil, "transactional IDs to match ACLs for (repeatable)")

	cmd.Flags().StringVar(&a.resourcePatternType, patternFlag, "literal", "pattern to use when matching resource names (any, match, literal, or prefixed)")

	cmd.Flags().StringSliceVar(&a.operations, operationFlag, nil, "operation to match (repeatable)")

	cmd.Flags().StringSliceVar(&a.allowPrincipals, allowPrincipalFlag, nil, "allowed principal ACLs to match (repeatable)")
	cmd.Flags().StringSliceVar(&a.allowHosts, allowHostFlag, nil, "allowed host ACLs to match (repeatable)")
	cmd.Flags().StringSliceVar(&a.denyPrincipals, denyPrincipalFlag, nil, "denied principal ACLs to match (repeatable)")
	cmd.Flags().StringSliceVar(&a.denyHosts, denyHostFlag, nil, "denied host ACLs to match (repeatable)")

	cmd.Flags().StringSliceVar(&a.any, anyFlag, nil, "flags to opt into matching anything for (e.g., --any=topic,group,allow-host or --any=all)")
}

func describeReqResp(
	cl *kgo.Client, printAllFilters bool, describes []*kmsg.DescribeACLsRequest,
) {
	// Kafka's DescribeACLsRequest actually only describes one filter at a
	// time. We issue all our filters concurrently and wait.
	var (
		wg      sync.WaitGroup
		results = make([]describeRespAndErr, len(describes))
	)
	for i := range describes {
		describe := describes[i]
		wg.Add(1)
		idx := i
		go func() {
			defer wg.Done()
			resp, err := describe.RequestWith(context.Background(), cl)
			results[idx] = describeRespAndErr{resp, err}
		}()
	}
	wg.Wait()

	// If any filters failed, or if all filters are
	// requested, we print the filter section.
	var printFailedFilters bool
	for _, r := range results {
		if r.err != nil || r.resp.ErrorCode != 0 {
			printFailedFilters = true
			break
		}
	}
	if printAllFilters || printFailedFilters {
		out.Section("filters")
		printDescribeFilters(printAllFilters, describes, results)
		fmt.Println()
		out.Section("matches")
	}

	printDescribedACLs(results)
}

type describeRespAndErr struct {
	resp *kmsg.DescribeACLsResponse
	err  error
}

func printDescribeFilters(
	all bool, reqs []*kmsg.DescribeACLsRequest, results []describeRespAndErr,
) {
	tw := out.NewTable(headersWithError...)
	defer tw.Flush()
	for i, res := range results {
		req := reqs[i]
		if !all && res.err == nil && res.resp.ErrorCode == 0 {
			continue
		}
		var msg string
		if res.err != nil {
			msg = res.err.Error()
		} else {
			msg = kafka.MaybeErrMessage(res.resp.ErrorCode)
		}
		tw.PrintStructFields(aclWithMessage{
			unptr(req.Principal),
			unptr(req.Host),
			req.ResourceType,
			unptr(req.ResourceName),
			req.ResourcePatternType,
			req.Operation,
			req.PermissionType,
			msg,
		})
	}
}

func printDescribedACLs(results []describeRespAndErr) {
	var described []acl
	for _, res := range results {
		if res.err != nil {
			continue
		}
		for _, resource := range res.resp.Resources {
			for _, a := range resource.ACLs {
				described = append(described, acl{
					Principal:           a.Principal,
					Host:                a.Host,
					ResourceType:        resource.ResourceType,
					ResourceName:        resource.ResourceName,
					ResourcePatternType: resource.ResourcePatternType,
					Operation:           a.Operation,
					Permission:          a.PermissionType,
				})
			}
		}
	}
	types.DistinctInPlace(&described)
	tw := out.NewTable(headers...)
	defer tw.Flush()
	for _, d := range described {
		tw.PrintStructFields(d)
	}
}
