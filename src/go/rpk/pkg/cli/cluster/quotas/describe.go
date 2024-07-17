// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package quotas

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/kafka"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
)

type quotaValue struct {
	Key   string `json:"key" yaml:"key"`
	Value string `json:"value" yaml:"value"`
}
type describedQuota struct {
	Entity []entityData `json:"entity" yaml:"entity"`
	Values []quotaValue `json:"values" yaml:"values"`

	entityStr string
}

type describeResponse struct {
	DescribedQuotas []describedQuota `json:"quotas,omitempty" yaml:"quotas,omitempty"`
}

func describeCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var (
		names    []string
		defaults []string
		anyFlag  []string
		strict   bool
	)
	cmd := &cobra.Command{
		Use:   "describe",
		Args:  cobra.NoArgs,
		Short: "Describe client quotas",
		Long: `Describe client quotas.

This command describes client quotas that match the provided filtering criteria.
Running the command without filters returns all client quotas. Use the 
'--strict' flag for strict matching, which means that the only quotas returned 
exactly match the filters.

Filters can be provided in terms of entities. An entity consists of either a 
client ID or a client ID prefix.
`,
		Example: `
Describe all client quotas:
  rpk cluster quotas describe

Describe all client quota with client ID foo:
  rpk cluster quotas describe --name client-id=foo

Describe client quotas for a given client ID prefix 'bar.':
  rpk cluster quotas describe --name client-id=bar.
`,
		Run: func(cmd *cobra.Command, _ []string) {
			f := p.Formatter
			if h, ok := f.Help(describeResponse{}); ok {
				out.Exit(h)
			}
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)

			adm, err := kafka.NewAdmin(fs, p)
			out.MaybeDie(err, "unable to initialize kafka client: %v", err)
			defer adm.Close()

			var reqQuotas []kadm.DescribeClientQuotaComponent
			for _, name := range names {
				split := strings.SplitN(name, "=", 2)
				if len(split) != 2 {
					out.Die("--name flag %q missing value", split[0])
				}
				k, v := split[0], split[1]
				k = strings.ToLower(k)
				if !anyValidTypes[k] {
					out.Die("name type %q is invalid (allowed: client-id, client-id-prefix)", split[0])
				}
				reqQuotas = append(reqQuotas, kadm.DescribeClientQuotaComponent{
					Type:      k,
					MatchName: &v,
					MatchType: 0,
				})
			}
			for _, def := range defaults {
				k := strings.ToLower(def)
				if !defaultValidTypes[k] {
					out.Die("default type %q is invalid (allowed: client-id)", def)
				}
				reqQuotas = append(reqQuotas, kadm.DescribeClientQuotaComponent{
					Type:      k,
					MatchType: 1,
				})
			}
			for _, a := range anyFlag {
				k := strings.ToLower(a)
				if !anyValidTypes[k] {
					out.Die("'any' type %q is invalid (allowed: client-id, client-id-prefix)", a)
				}
				reqQuotas = append(reqQuotas, kadm.DescribeClientQuotaComponent{
					Type:      k,
					MatchType: 2,
				})
			}
			quotas, err := adm.DescribeClientQuotas(cmd.Context(), strict, reqQuotas)
			out.MaybeDie(err, "unable to describe client quotas: %v", err)

			err = printDescribedQuotas(f, quotas)
			out.MaybeDie(err, "unable to print described quotas: %v", err)
		},
	}

	cmd.Flags().StringSliceVar(&names, "name", nil, "type=name pair for exact name matching, where type is client-id or client-id-prefix (repeatable)")
	cmd.Flags().StringSliceVar(&defaults, "default", nil, "type for default matching, where type is client-id or client-id-prefix (repeatable)")
	cmd.Flags().StringSliceVar(&anyFlag, "any", nil, "type for any matching (names or default), where type is client-id or client-id-prefix (repeatable)")
	cmd.Flags().BoolVar(&strict, "strict", false, "whether matches are strict, if true, entities with unspecified entity types are excluded")

	return cmd
}

func printDescribedQuotas(f config.OutFormatter, quotas []kadm.DescribedClientQuota) error {
	var described []describedQuota
	for _, q := range quotas {
		entity, entityStr := parseEntityData(q.Entity)
		var qv []quotaValue
		for _, v := range q.Values {
			qv = append(qv, quotaValue{
				Key:   v.Key,
				Value: strconv.FormatFloat(v.Value, 'f', -1, 64),
			})
		}
		described = append(described, describedQuota{
			Entity:    entity,
			Values:    qv,
			entityStr: entityStr,
		})
	}
	if isText, _, s, err := f.Format(describeResponse{described}); !isText {
		if err != nil {
			return fmt.Errorf("unable to print in the required format %q: %v", f.Kind, err)
		}
		fmt.Println(s)
		return nil
	}
	if len(described) == 0 {
		fmt.Println("no quotas matched to describe")
		return nil
	}
	for i, d := range described {
		fmt.Println(d.entityStr)
		for _, qv := range d.Values {
			fmt.Printf("\t%v=%v\n", qv.Key, qv.Value)
		}
		if i < len(described)-1 {
			fmt.Println()
		}
	}
	return nil
}
