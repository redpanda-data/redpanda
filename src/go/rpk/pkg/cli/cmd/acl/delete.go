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
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/types"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/kafka"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
)

func NewDeleteCommand(fs afero.Fs) *cobra.Command {
	var (
		a               acls
		printAllFilters bool
		dry             bool
		noConfirm       bool
	)
	cmd := &cobra.Command{
		Use:   "delete",
		Short: "Delete ACLs.",
		Long: `Delete ACLs.

See the 'rpk acl' help text for a full write up on ACLs. Delete flags work in a
similar multiplying effect as creating ACLs, but delete is more advanced:
deletion works on a filter basis. Any unspecified flag defaults to matching
everything (all operations, or all allowed principals, etc). To ensure that you
do not accidentally delete more than you intend, this command prints everything
that matches your input filters and prompts for a confirmation before the
delete request is issued. Anything matching more than 10 ACLs doubly confirms.

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

			b, err := a.createDeletionsAndDescribes(false)
			out.MaybeDieErr(err)

			var printDeletionsHeader bool
			if !noConfirm || dry {
				describeReqResp(adm, printAllFilters, true, b)
				fmt.Println()

				confirmed, err := out.Confirm("Confirm deletion of the above matching ACLs?")
				out.MaybeDie(err, "unable to confirm deletion: %v", err)
				if !confirmed {
					out.Exit("Deletion canceled.")
				}
				if dry {
					fmt.Println("Dry run, exiting.")
					return
				}
				fmt.Println()

				// If the user opted in to printing filters, we
				// just did. Disable printing filters again,
				// unless some filter on delete errors.
				printAllFilters = false
				printDeletionsHeader = true
			}

			deleteReqResp(adm, printAllFilters, printDeletionsHeader, b)
		},
	}
	a.addDeleteFlags(cmd)
	cmd.Flags().BoolVarP(&printAllFilters, "print-filters", "f", false, "print the filters that were requested (failed filters are always printed)")
	cmd.Flags().BoolVarP(&dry, "dry", "d", false, "dry run: validate what would be deleted")
	cmd.Flags().BoolVar(&noConfirm, "no-confirm", false, "disable confirmation prompt")
	return cmd
}

func (a *acls) addDeleteFlags(cmd *cobra.Command) {
	a.addDeprecatedFlags(cmd)

	cmd.Flags().StringSliceVar(&a.topics, topicFlag, nil, "topic to remove ACLs for (repeatable)")
	cmd.Flags().StringSliceVar(&a.groups, groupFlag, nil, "group to remove ACLs for (repeatable)")
	cmd.Flags().BoolVar(&a.cluster, clusterFlag, false, "whether to remove ACLs to the cluster")
	cmd.Flags().StringSliceVar(&a.txnIDs, txnIDFlag, nil, "transactional IDs to remove ACLs for (repeatable)")

	cmd.Flags().StringVar(&a.resourcePatternType, patternFlag, "any", "pattern to use when matching resource names (any, match, literal, or prefixed)")

	cmd.Flags().StringSliceVar(&a.operations, operationFlag, nil, "operation to remove (repeatable)")

	cmd.Flags().StringSliceVar(&a.allowPrincipals, allowPrincipalFlag, nil, "allowed principal ACLs to remove (repeatable)")
	cmd.Flags().StringSliceVar(&a.allowHosts, allowHostFlag, nil, "allowed host ACLs to remove (repeatable)")
	cmd.Flags().StringSliceVar(&a.denyPrincipals, denyPrincipalFlag, nil, "denied principal ACLs to remove (repeatable)")
	cmd.Flags().StringSliceVar(&a.denyHosts, denyHostFlag, nil, "denied host ACLs to remove (repeatable)")
}

func deleteReqResp(
	adm *kadm.Client,
	printAllFilters bool,
	printDeletionsHeader bool,
	b *kadm.ACLBuilder,
) {
	results, err := adm.DeleteACLs(context.Background(), b)
	out.MaybeDie(err, "unable to delete ACLs: %v", err)
	types.Sort(results)

	// If any filters failed, or if all filters are requested, we print the
	// filter section.
	var printFailedFilters bool
	for _, f := range results {
		if f.Err != nil {
			printFailedFilters = true
			break
		}
	}
	if printAllFilters || printFailedFilters {
		out.Section("filters")
		printDeleteFilters(printAllFilters, results)
		fmt.Println()
		printDeletionsHeader = true

	}
	if printDeletionsHeader {
		out.Section("deletions")
	}
	printDeleteResults(results)
}

func printDeleteFilters(all bool, results kadm.DeleteACLsResults) {
	tw := out.NewTable(headersWithError...)
	defer tw.Flush()
	for _, f := range results {
		if f.Err == nil && !all {
			continue
		}
		tw.PrintStructFields(aclWithMessage{
			unptr(f.Principal),
			unptr(f.Host),
			f.Type,
			unptr(f.Name),
			f.Pattern,
			f.Operation,
			f.Permission,
			kafka.ErrMessage(f.Err),
		})
	}
}

func printDeleteResults(results kadm.DeleteACLsResults) {
	tw := out.NewTable(headersWithError...)
	defer tw.Flush()
	for _, f := range results {
		for _, d := range f.Deleted {
			tw.PrintStructFields(aclWithMessage{
				d.Principal,
				d.Host,
				d.Type,
				d.Name,
				d.Pattern,
				d.Operation,
				d.Permission,
				kafka.ErrMessage(d.Err),
			})
		}
	}
}
