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
	"encoding/json"
	"fmt"
	"os"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/kafka"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/types"
)

func newListCommand(fs afero.Fs) *cobra.Command {
	var (
		a               acls
		printAllFilters bool
		printJSON       bool
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
			describeReqResp(adm, printAllFilters, false, b, printJSON)
		},
	}
	a.addListFlags(cmd)
	cmd.Flags().BoolVarP(&printJSON, "json-output", "j", false, "Print acl list as json.")
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

// Add json tags to kadm.DescribeACLsResult
// And remove the Described slice
type JsonDescribeACLsResult struct {
	Principal *string `json:"principal"` // Principal is the optional user that was used in this filter.
	Host      *string `json:"host"`      // Host is the optional host that was used in this filter.

	Type       kmsg.ACLResourceType   `json:"type"`       // Type is the type of resource used for this filter.
	Name       *string                `json:"name"`       // Name is the name of the resource used for this filter.
	Pattern    kadm.ACLPattern        `json:"pattern"`    // Pattern is the name pattern used for this filter.
	Operation  kmsg.ACLOperation      `json:"operation"`  // Operation is the operation used for this filter.
	Permission kmsg.ACLPermissionType `json:"permission"` // Permission is permission used for this filter.

	Err error // Err is non-nil if this filter has an error.
}

// Add json tags to kadm.JsonDescribedACL
type JsonDescribedACL struct {
	Principal string `json:"principal"` // Principal is this described ACL's principal.
	Host      string `json:"host"`      // Host is this described ACL's host.

	Type       kmsg.ACLResourceType   `json:"type"`       // Type is this described ACL's resource type.
	Name       string                 `json:"name"`       // Name is this described ACL's resource name.
	Pattern    kadm.ACLPattern        `json:"pattern"`    // Pattern is this described ACL's resource name pattern.
	Operation  kmsg.ACLOperation      `json:"operation"`  // Operation is this described ACL's operation.
	Permission kmsg.ACLPermissionType `json:"permission"` // Permission this described ACLs permission.
}

type JsonFilterAndDescribed struct {
	Filter JsonDescribeACLsResult `json:"filter"`
	Acls   []JsonDescribedACL     `json:"described_acls"`
}

type JsonFilterAndDescribedResults struct {
	Results []JsonFilterAndDescribed `json:"results"`
}

func (collection *JsonFilterAndDescribedResults) addFilterAndDescribed(data JsonFilterAndDescribed) {
	collection.Results = append(collection.Results, data)
}

func describeReqResp(
	adm *kadm.Client,
	printAllFilters bool,
	printMatchesHeader bool,
	b *kadm.ACLBuilder,
	printJSON bool,
) {
	results, err := adm.DescribeACLs(context.Background(), b)
	out.MaybeDie(err, "unable to list ACLs: %v", err)
	types.Sort(results)

	// If any filters failed, or if all filters are
	// requested, we print the filter section.
	printFailedFilters := false
	for _, f := range results {
		if f.Err != nil {
			printFailedFilters = true
			break
		}
	}

	printFilters := false
	if printAllFilters || printFailedFilters {
		printFilters = true
	}

	if printJSON {
		JsonFilterAndDescribedResults := JsonFilterAndDescribedResults{}

		for _, result := range results {
			// Filter portion of the result
			newACL := JsonFilterAndDescribed{
				Filter: JsonDescribeACLsResult{
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
			// Acl lice portion of the result
			// Init slice to 0 length so if nothing matches filter, json Marshal will return [] instead of NULL
			newACL.Acls = []JsonDescribedACL{}
			for _, described := range result.Described {
				newACL.Acls = append(newACL.Acls,
					JsonDescribedACL{
						Principal:  described.Principal,
						Host:       described.Host,
						Type:       described.Type,
						Name:       described.Name,
						Pattern:    described.Pattern,
						Operation:  described.Operation,
						Permission: described.Permission,
					},
				)
			}
			JsonFilterAndDescribedResults.addFilterAndDescribed(newACL)

		}
		jsonBytes, err := json.Marshal(JsonFilterAndDescribedResults)
		if err != nil {
			fmt.Printf("Failed to martial json for output. Error: %s", err)
			os.Exit(1)
		}
		fmt.Println(string(jsonBytes))
		return
	}

	if printFilters {
		out.Section("filters")
		printDescribeFilters(results)
		fmt.Println()
		printMatchesHeader = true
	}

	if printMatchesHeader {
		out.Section("matches")
	}
	printDescribedACLs(results)
}

func printDescribeFilters(results kadm.DescribeACLsResults) {
	tw := out.NewTable(headersWithError...)
	defer tw.Flush()
	for _, f := range results {
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

func printDescribedACLs(results kadm.DescribeACLsResults) {
	tw := out.NewTable(headersWithError...)
	defer tw.Flush()
	for _, f := range results {
		for _, d := range f.Described {
			tw.PrintStructFields(acl{
				d.Principal,
				d.Host,
				d.Type,
				d.Name,
				d.Pattern,
				d.Operation,
				d.Permission,
			})
		}
	}
}
