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
	"io"

	"go.uber.org/zap"

	"github.com/redpanda-data/common-go/rpsr"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/kafka"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/schemaregistry"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/types"
)

func newDeleteCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var (
		a               acls
		printAllFilters bool
		dry             bool
		noConfirm       bool
	)
	cmd := &cobra.Command{
		Use:   "delete",
		Short: "Delete ACLs",
		Long: `Delete ACLs.

See the 'rpk security acl' help text for a full write up on ACLs. Delete flags
work in a similar multiplying effect as creating ACLs, but delete is more
advanced: deletion works on a filter basis. Any unspecified flag defaults to
matching everything (all operations, or all allowed principals, etc). To ensure
that you do not accidentally delete more than you intend, this command prints
everything that matches your input filters and prompts for a confirmation before
the delete request is issued. Anything matching more than 10 ACLs doubly
confirms.

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
			f := p.Formatter
			if h, ok := f.Help(&aclDeleteOutput{}); ok {
				out.Exit(h)
			}
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)

			adm, err := kafka.NewAdmin(fs, p)
			out.MaybeDie(err, "unable to initialize kafka client: %v", err)
			defer adm.Close()

			srClient, err := schemaregistry.NewClient(fs, p)
			out.MaybeDie(err, "unable to initialize schema registry client: %v", err)

			isKafka, isSR, err := kafkaOrSRFilters(cmd, false)
			out.MaybeDie(err, "invalid request: %v", err)

			kBuilder, srACLs, err := a.createDeletionsAndDescribes(false, isKafka, isSR)
			out.MaybeDieErr(err)

			var printDeletionsHeader bool
			var filteredSRACLs []rpsr.ACL
			if !noConfirm || dry {
				filteredSRACLs = describeReqResp(cmd.Context(), adm, srClient, printAllFilters, true, kBuilder, srACLs, f)
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
			deleteReqResp(cmd.Context(), adm, srClient, printAllFilters, printDeletionsHeader, kBuilder, srACLs, filteredSRACLs, f, cmd.OutOrStdout())
		},
	}
	p.InstallFormatFlag(cmd)
	p.InstallKafkaFlags(cmd)
	a.addDeleteFlags(cmd)
	cmd.Flags().BoolVarP(&printAllFilters, "print-filters", "f", false, "Print the filters that were requested (failed filters are always printed)")
	cmd.Flags().BoolVarP(&dry, "dry", "d", false, "Dry run: validate what would be deleted")
	cmd.Flags().BoolVar(&noConfirm, "no-confirm", false, "Disable confirmation prompt")
	return cmd
}

func (a *acls) addDeleteFlags(cmd *cobra.Command) {
	a.addDeprecatedFlags(cmd)

	cmd.Flags().StringSliceVar(&a.topics, topicFlag, nil, "Topic to remove ACLs for (repeatable)")
	cmd.Flags().StringSliceVar(&a.groups, groupFlag, nil, "Group to remove ACLs for (repeatable)")
	cmd.Flags().BoolVar(&a.cluster, clusterFlag, false, "Whether to remove ACLs for the cluster")
	cmd.Flags().StringSliceVar(&a.txnIDs, txnIDFlag, nil, "Transactional IDs to remove ACLs for (repeatable)")
	cmd.Flags().BoolVar(&a.registry, registryFlag, false, "Whether to remove ACLs for the schema registry")
	cmd.Flags().StringSliceVar(&a.subjects, subjectFlag, nil, "Schema Registry subjects to remove ACLs for (repeatable)")

	cmd.Flags().StringVar(&a.resourcePatternType, patternFlag, "any", "Pattern to use when matching resource names (any, match, literal, or prefixed)")
	cmd.Flags().StringSliceVar(&a.operations, operationFlag, nil, "Operation to remove (repeatable)")

	cmd.Flags().StringSliceVar(&a.allowPrincipals, allowPrincipalFlag, nil, "Allowed principal ACLs to remove (repeatable)")
	cmd.Flags().StringSliceVar(&a.allowRoles, allowRoleFlag, nil, "Allowed role to remove this ACL from (repeatable)")
	cmd.Flags().StringSliceVar(&a.allowHosts, allowHostFlag, nil, "Allowed host ACLs to remove (repeatable)")
	cmd.Flags().StringSliceVar(&a.denyPrincipals, denyPrincipalFlag, nil, "Denied principal ACLs to remove (repeatable)")
	cmd.Flags().StringSliceVar(&a.denyRoles, denyRoleFlag, nil, "Denied role to remove this ACL from (repeatable)")
	cmd.Flags().StringSliceVar(&a.denyHosts, denyHostFlag, nil, "Denied host ACLs to remove (repeatable)")
}

type aclDeleteOutput struct {
	Filters   []aclWithMessage `json:"filters,omitempty" yaml:"filters,omitempty"`
	Deletions []aclWithMessage `json:"deletions" yaml:"deletions"`
}

func deleteReqResp(
	ctx context.Context,
	adm *kadm.Client,
	srCl *rpsr.Client,
	printAllFilters bool,
	printDeletionsHeader bool,
	b *kadm.ACLBuilder,
	srACLsFilter []rpsr.ACL,
	filteredSRACLs []rpsr.ACL,
	f config.OutFormatter,
	w io.Writer,
) {
	var (
		kResults  []kadm.DeleteACLsResult
		srResults []rpsr.ACL
		err       error
		srErr     error
	)
	if b != nil {
		kResults, err = adm.DeleteACLs(ctx, b)
		out.MaybeDie(err, "unable to delete Kafka ACLs: %v", err)
	}
	// Kafka ACLs deletion is done, and the result already contains the error
	// message for each row. For SR we store a single error message and display
	// it at the end.
	if srACLsFilter != nil {
		if filteredSRACLs == nil {
			filteredSRACLs, err = srCl.ListACLsBatch(ctx, srACLsFilter)
			if err != nil {
				zap.L().Sugar().Warnf("failed to list schema registry ACLs: %v", err)
				srErr = fmt.Errorf("unable to list sr ACLs: %v", err)
			}
		}
		err = srCl.DeleteACLs(ctx, filteredSRACLs)
		if err != nil {
			srErr = fmt.Errorf("unable to delete Schema Registry ACLs: %v", err)
		}
		srResults = filteredSRACLs
	}

	// Build the structured output.
	output := aclDeleteOutput{
		Deletions: []aclWithMessage{},
	}

	// If any filters failed, or if all filters are requested, include them.
	var printFailedFilters bool
	for _, r := range kResults {
		if r.Err != nil {
			printFailedFilters = true
			break
		}
	}
	if printAllFilters || printFailedFilters {
		for _, r := range kResults {
			if r.Err == nil && !printAllFilters {
				continue
			}
			output.Filters = append(output.Filters, aclWithMessage{
				unptr(r.Principal),
				unptr(r.Host),
				r.Type.String(),
				unptr(r.Name),
				r.Pattern.String(),
				r.Operation.String(),
				r.Permission.String(),
				kafka.ErrMessage(r.Err),
			})
		}
		for _, r := range srACLsFilter {
			msg := ""
			if srErr != nil {
				msg = srErr.Error()
			}
			output.Filters = append(output.Filters, aclWithMessage{
				Principal:           r.Principal,
				Host:                r.Host,
				ResourceType:        string(r.ResourceType),
				ResourceName:        r.Resource,
				ResourcePatternType: string(r.PatternType),
				Operation:           string(r.Operation),
				Permission:          string(r.Permission),
				Message:             msg,
			})
		}
	}

	for _, r := range kResults {
		for _, d := range r.Deleted {
			output.Deletions = append(output.Deletions, aclWithMessage{
				d.Principal,
				d.Host,
				d.Type.String(),
				d.Name,
				d.Pattern.String(),
				d.Operation.String(),
				d.Permission.String(),
				kafka.ErrMessage(d.Err),
			})
		}
	}
	for _, r := range srResults {
		msg := ""
		if srErr != nil {
			msg = srErr.Error()
		}
		output.Deletions = append(output.Deletions, aclWithMessage{
			Principal:           r.Principal,
			Host:                r.Host,
			ResourceType:        string(r.ResourceType),
			ResourceName:        r.Resource,
			ResourcePatternType: string(r.PatternType),
			Operation:           string(r.Operation),
			Permission:          string(r.Permission),
			Message:             msg,
		})
	}

	types.Sort(output)

	// Presence of filters implies a deletions section header is needed.
	if len(output.Filters) > 0 {
		printDeletionsHeader = true
	}
	printDeleteOutput(f, output, printDeletionsHeader, w)
}

func printDeleteOutput(f config.OutFormatter, output aclDeleteOutput, printDeletionsHeader bool, w io.Writer) {
	if isText, _, t, err := f.Format(&output); !isText {
		out.MaybeDie(err, "unable to print in the requested format %q: %v", f.Kind, err)
		fmt.Fprintf(w, "%s\n", t)
		return
	}
	if len(output.Filters) > 0 {
		out.SectionTo(w, "filters")
		tw := out.NewTableTo(w, headersWithError...)
		for _, r := range output.Filters {
			tw.PrintStructFields(r)
		}
		tw.Flush()
		fmt.Fprintln(w)
	}
	if printDeletionsHeader {
		out.SectionTo(w, "deletions")
	}
	tw := out.NewTableTo(w, headersWithError...)
	for _, r := range output.Deletions {
		tw.PrintStructFields(r)
	}
	tw.Flush()
}
