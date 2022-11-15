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
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/types"
)

func newListCommand(fs afero.Fs) *cobra.Command {
	var (
		a               acls
		printAllFilters bool
		format          string
	)
	cmd := &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls", "describe"},
		Short:   "List ACLs",
		Long: `List ACLs.

See the 'rpk acl' help text for a full write up on ACLs. List flags work in a
similar multiplying effect as creating ACLs, but list is more advanced:
listing works on a filter basis. Any unspecified flag defaults to matching
everything (all operations, or all allowed principals, etc).

As mentioned, not specifying flags matches everything. If no resources are
specified, all resources are matched. If no operations are specified, all
operations are matched. You can also opt in to matching everything with "any":
--operation any matches any operation.

The --resource-pattern-type, defaulting to "any", configures how to filter
resource names:
  * "any" returns exact name matches of either prefixed or literal pattern type
  * "match" returns wildcard matches, prefix patterns that match your input, and literal matches
  * "prefix" returns prefix patterns that match your input (prefix "fo" matches "foo")
  * "literal" returns exact name matches
`,
		Args: cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, _ []string) {
			p := config.ParamsFromCommand(cmd)
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			adm, err := kafka.NewAdmin(fs, p, cfg)
			out.MaybeDie(err, "unable to initialize kafka client: %v", err)
			defer adm.Close()

			b, err := a.createDeletionsAndDescribes(true)
			out.MaybeDieErr(err)
			describeReqResp(adm, printAllFilters, false, b, format)
		},
	}
	a.addListFlags(cmd)
	cmd.Flags().StringVar(&format, "format", "text", "Output format (text, json, yaml)")
	cmd.Flags().BoolVarP(&printAllFilters, "print-filters", "f", false, "Print the filters that were requested (failed filters are always printed)")
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

	cmd.Flags().StringSliceVar(&a.topics, topicFlag, nil, "Topic to match ACLs for (repeatable)")
	cmd.Flags().StringSliceVar(&a.groups, groupFlag, nil, "Group to match ACLs for (repeatable)")
	cmd.Flags().BoolVar(&a.cluster, clusterFlag, false, "Whether to match ACLs to the cluster")
	cmd.Flags().StringSliceVar(&a.txnIDs, txnIDFlag, nil, "Transactional IDs to match ACLs for (repeatable)")

	cmd.Flags().StringVar(&a.resourcePatternType, patternFlag, "any", "Pattern to use when matching resource names (any, match, literal, or prefixed)")

	cmd.Flags().StringSliceVar(&a.operations, operationFlag, nil, "Operation to match (repeatable)")

	cmd.Flags().StringSliceVar(&a.allowPrincipals, allowPrincipalFlag, nil, "Allowed principal ACLs to match (repeatable)")
	cmd.Flags().StringSliceVar(&a.allowHosts, allowHostFlag, nil, "Allowed host ACLs to match (repeatable)")
	cmd.Flags().StringSliceVar(&a.denyPrincipals, denyPrincipalFlag, nil, "Denied principal ACLs to match (repeatable)")
	cmd.Flags().StringSliceVar(&a.denyHosts, denyHostFlag, nil, "Denied host ACLs to match (repeatable)")
}

func describeReqResp(
	adm *kadm.Client,
	printAllFilters bool,
	printMatchesHeader bool,
	b *kadm.ACLBuilder,
	format string,
) {
	results, err := adm.DescribeACLs(context.Background(), b)
	out.MaybeDie(err, "unable to list ACLs: %v", err)
	types.Sort(results)

	filteredAndDescribedResults := filteredAndDescribedResults{}
	for _, result := range results {
		// Filter portion of the result
		newACL := filteredAndDescribed{
			Filter: describedACLsResult{
				Principal:  result.Principal,
				Host:       result.Host,
				Type:       result.Type,
				Name:       result.Name,
				Pattern:    result.Pattern,
				Operation:  result.Operation,
				Permission: result.Permission,
				Err:        result.Err,
			},
		}
		// Acl portion of the result
		// Init slice to 0 length so if nothing matches filter, json Marshal will return [] instead of NULL
		newACL.ACLS = []acl{}
		for _, described := range result.Described {
			newACL.ACLS = append(newACL.ACLS,
				acl{
					Principal:           described.Principal,
					Host:                described.Host,
					ResourceType:        described.Type,
					ResourceName:        described.Name,
					ResourcePatternType: described.Pattern,
					Operation:           described.Operation,
					Permission:          described.Permission,
				},
			)
		}
		filteredAndDescribedResults.addFilterAndDescribed(newACL)
	}

	if format != "text" {
		out.StructredPrint[any](filteredAndDescribedResults, format)
	} else {
		// If any filters failed, or if all filters are
		// requested, we print the filter section.
		printFailedFilters := false
		for _, f := range filteredAndDescribedResults.Results {
			if f.Filter.Err != nil {
				printFailedFilters = true
				break
			}
		}

		printFilters := false
		if printAllFilters || printFailedFilters {
			printFilters = true
		}

		if printFilters {
			out.Section("filters")
			printDescribeFilters(filteredAndDescribedResults)
			fmt.Println()
			printMatchesHeader = true
		}

		if printMatchesHeader {
			out.Section("matches")
		}
		printDescribedACLs(filteredAndDescribedResults)
	}
}

func printDescribeFilters(filteredResultsCollection filteredAndDescribedResults) {
	tw := out.NewTable(headersWithError...)
	defer tw.Flush()
	for _, f := range filteredResultsCollection.Results {
		tw.PrintStructFields(aclWithMessage{
			unptr(f.Filter.Principal),
			unptr(f.Filter.Host),
			f.Filter.Type,
			unptr(f.Filter.Name),
			f.Filter.Pattern,
			f.Filter.Operation,
			f.Filter.Permission,
			kafka.ErrMessage(f.Filter.Err),
		})
	}
}

func printDescribedACLs(filteredResultsCollection filteredAndDescribedResults) {
	tw := out.NewTable(headersWithError...)
	defer tw.Flush()
	for _, f := range filteredResultsCollection.Results {
		for _, d := range f.ACLS {
			tw.PrintStructFields(d)
		}
	}
}
