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

func newListCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var (
		a               acls
		printAllFilters bool
		subsystem       []string
	)
	cmd := &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls", "describe"},
		Short:   "List ACLs",
		Long: `List ACLs.

See the 'rpk security acl' help text for a full write up on ACLs. List flags
work in a similar multiplying effect as creating ACLs, but list is more
advanced: listing works on a filter basis. Any unspecified flag defaults to
matching everything (all operations, or all allowed principals, etc).

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

The list command lists ACLs for both Kafka and Schema Registry. To limit the 
results to a specific subsystem, use the --subsystem flag with either "kafka" or 
"registry".
`,
		Example: `
List all ACLs:
  rpk security acl list

List all Schema Registry ACLs:
  rpk security acl list --subsystem registry

List all ACLs for topic "foo":
  rpk security acl list --topic foo

List all ACLs for user "bar" on topic "foo":
  rpk security acl list --allow-principal bar --topic foo

List all ACLs for role "admin" on schema registry subject "foo-value":
  rpk security acl list --allow-role admin --registry-subject foo-value
`,
		Args: cobra.NoArgs,
		Run: func(cmd *cobra.Command, _ []string) {
			f := p.Formatter
			if h, ok := f.Help(&aclListOutput{}); ok {
				out.Exit(h)
			}
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)

			adm, err := kafka.NewAdmin(fs, p)
			out.MaybeDie(err, "unable to initialize kafka client: %v", err)
			defer adm.Close()

			srClient, err := schemaregistry.NewClient(fs, p)
			out.MaybeDie(err, "unable to initialize schema registry client: %v", err)

			isKafka, isSR, err := kafkaOrSRFilters(cmd, true)
			out.MaybeDie(err, "invalid request: %v", err)

			kBuilder, srACLs, err := a.createDeletionsAndDescribes(true, isKafka, isSR)
			out.MaybeDieErr(err)
			describeReqResp(cmd.Context(), adm, srClient, printAllFilters, false, kBuilder, srACLs, f)
		},
	}
	p.InstallFormatFlag(cmd)
	p.InstallKafkaFlags(cmd)
	a.addListFlags(cmd)
	cmd.Flags().StringSliceVar(&subsystem, subsystemFlag, []string{"kafka", "registry"}, "The subsystem to match ACLs for (kafka, registry, or both)")
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
	cmd.Flags().BoolVar(&a.registry, registryFlag, false, "Whether to grant ACLs for the schema registry")
	cmd.Flags().StringSliceVar(&a.subjects, subjectFlag, nil, "Schema Registry subjects to grant ACLs for (repeatable)")

	cmd.Flags().StringVar(&a.resourcePatternType, patternFlag, "any", "Pattern to use when matching resource names (any, match, literal, or prefixed)")
	cmd.Flags().StringSliceVar(&a.operations, operationFlag, nil, "Operation to match (repeatable)")

	cmd.Flags().StringSliceVar(&a.allowPrincipals, allowPrincipalFlag, nil, "Allowed principal ACLs to match (repeatable)")
	cmd.Flags().StringSliceVar(&a.allowRoles, allowRoleFlag, nil, "Allowed role ACLs to match (repeatable)")
	cmd.Flags().StringSliceVar(&a.allowHosts, allowHostFlag, nil, "Allowed host ACLs to match (repeatable)")
	cmd.Flags().StringSliceVar(&a.denyPrincipals, denyPrincipalFlag, nil, "Denied principal ACLs to match (repeatable)")
	cmd.Flags().StringSliceVar(&a.denyRoles, denyRoleFlag, nil, "Denied role ACLs to match (repeatable)")
	cmd.Flags().StringSliceVar(&a.denyHosts, denyHostFlag, nil, "Denied host ACLs to match (repeatable)")
}

func describeReqResp(
	ctx context.Context,
	adm *kadm.Client,
	srClient *rpsr.Client,
	printAllFilters bool,
	printMatchesHeader bool,
	b *kadm.ACLBuilder,
	srACLs []rpsr.ACL,
	f config.OutFormatter,
) []rpsr.ACL {
	var (
		kResults  []kadm.DescribeACLsResult
		srResults []rpsr.ACL
		err       error
		srErr     error
	)
	if b != nil {
		kResults, err = adm.DescribeACLs(ctx, b)
		out.MaybeDie(err, "unable to list kafka ACLs: %v", err)
	}

	if srACLs != nil {
		srResults, err = srClient.ListACLsBatch(ctx, srACLs)
		if err != nil {
			// For backwards compatibility, we print the error instead of
			// exiting.
			srErr = fmt.Errorf("unable to list sr ACLs: %v", err)
			zap.L().Sugar().Warnf("failed to list schema registry ACLs: %v", err)
		}
	}

	// If any filters failed, or if all filters are
	// requested, we print the filter section.
	var printFailedFilters bool
	for _, f := range kResults {
		if f.Err != nil {
			printFailedFilters = true
			break
		}
	}
	// Intentionally starting 'Matches' with an empty slice to avoid printing
	// null when marshalling to JSON.
	output := aclListOutput{
		Matches: []acl{},
	}
	for _, f := range kResults {
		if printAllFilters || printFailedFilters {
			output.Filters = append(output.Filters, aclWithMessage{
				unptr(f.Principal),
				unptr(f.Host),
				f.Type.String(),
				unptr(f.Name),
				f.Pattern.String(),
				f.Operation.String(),
				f.Permission.String(),
				kafka.ErrMessage(f.Err),
			})
		}
		for _, d := range f.Described {
			output.Matches = append(output.Matches, acl{
				d.Principal,
				d.Host,
				d.Type.String(),
				d.Name,
				d.Pattern.String(),
				d.Operation.String(),
				d.Permission.String(),
			})
		}
	}
	if printAllFilters || printFailedFilters {
		for _, f := range srACLs {
			msg := ""
			if srErr != nil {
				msg = srErr.Error()
			}
			output.Filters = append(output.Filters, aclWithMessage{
				Principal:           f.Principal,
				Host:                f.Host,
				ResourceType:        string(f.ResourceType),
				ResourceName:        f.Resource,
				ResourcePatternType: string(f.PatternType),
				Operation:           string(f.Operation),
				Permission:          string(f.Permission),
				Message:             msg,
			})
		}
	}
	for _, f := range srResults {
		output.Matches = append(output.Matches, acl{
			Principal:           f.Principal,
			Host:                f.Host,
			ResourceType:        string(f.ResourceType),
			ResourceName:        f.Resource,
			ResourcePatternType: string(f.PatternType),
			Operation:           string(f.Operation),
			Permission:          string(f.Permission),
		})
	}

	types.Sort(output)

	if isText, _, t, err := f.Format(&output); !isText {
		out.MaybeDie(err, "unable to print in the requested format %q: %v", f.Kind, err)
		fmt.Printf("%s\n", t)
		return srResults
	}
	if len(output.Filters) > 0 {
		out.Section("filters")
		printDescribeFilters(output)
		fmt.Println()
		printMatchesHeader = true
	}
	if printMatchesHeader {
		out.Section("matches")
	}
	printDescribedACLs(output)
	return srResults
}

type aclListOutput struct {
	Filters []aclWithMessage `json:"filters,omitempty"`
	Matches []acl            `json:"matches"`
}

func printDescribeFilters(output aclListOutput) {
	tw := out.NewTable(headersWithError...)
	defer tw.Flush()
	for _, f := range output.Filters {
		tw.PrintStructFields(f)
	}
}

func printDescribedACLs(output aclListOutput) {
	tw := out.NewTable(headersWithError...)
	defer tw.Flush()
	for _, m := range output.Matches {
		tw.PrintStructFields(m)
	}
}
