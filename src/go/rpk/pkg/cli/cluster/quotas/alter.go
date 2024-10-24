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
	"go.uber.org/zap"
)

type createResponse struct {
	Entity []entityData `json:"entity" yaml:"entity"`
	Status string       `json:"status" yaml:"status"`

	entityStr string
}

func alterCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var (
		dry      bool
		names    []string
		defaults []string
		adds     []string
		deletes  []string
	)
	cmd := &cobra.Command{
		Use:   "alter",
		Args:  cobra.NoArgs,
		Short: "Add or delete a client quota",
		Long: `Add or delete a client quota

This command allows you to add or delete a client quota.

A client quota consists of an entity (to whom the quota is applied) and a quota 
type (what is being applied).

There are two entity types supported by Redpanda: client ID and client ID 
prefix.

Assigning quotas to default entity types is possible using the '--default' flag.

You can perform a dry run using the '--dry' flag.
`,
		Example: `
Add quota (consumer_byte_rate) to client ID 'foo':
  rpk cluster quotas alter --add consumer_byte_rate=200000 \
    --name client-id=foo

Add quota (consumer_byte_rate) to client ID starting with 'bar-':
  rpk cluster quotas alter --add consumer_byte_rate=200000 \
    --name client-id-prefix=bar-

Add quota (producer_byte_rate) to default client ID:
  rpk cluster quotas alter --add producer_byte_rate=180000 --default client-id

Remove quota (producer_byte_rate) from client ID 'foo':
  rpk cluster quotas alter --delete producer_byte_rate \
    --name client-id=foo
`,
		Run: func(cmd *cobra.Command, _ []string) {
			f := p.Formatter
			if h, ok := f.Help(createResponse{}); ok {
				out.Exit(h)
			}
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)

			adm, err := kafka.NewAdmin(fs, p)
			out.MaybeDie(err, "unable to initialize kafka client: %v", err)
			defer adm.Close()

			var (
				entity  []kadm.ClientQuotaEntityComponent
				nameMap = make(map[string]bool)
			)
			for _, name := range names {
				split := strings.SplitN(name, "=", 2)
				if len(split) != 2 {
					out.Die("name %q missing value", split[0])
				}
				k, v := split[0], split[1]
				k = strings.ToLower(k)
				if !anyValidTypes[k] {
					out.Die("name type %q is invalid (allowed: client-id, client-id-prefix)", split[0])
				}
				nameMap[k] = true
				entity = append(entity, kadm.ClientQuotaEntityComponent{
					Type: k,
					Name: &v,
				})
			}
			for _, def := range defaults {
				if !defaultValidTypes[def] {
					out.Die("default type %q is invalid (allowed: client-id)", def)
				}
				if nameMap[def] {
					out.Die("default type %q was previously defined in --name, you can only set it once", def)
				}
				entity = append(entity, kadm.ClientQuotaEntityComponent{
					Type: def,
				})
			}

			var operations []kadm.AlterClientQuotaOp
			for _, add := range adds {
				split := strings.SplitN(add, "=", 2)
				if len(split) != 2 {
					out.Die("missing value in flag --add: %q", add)
				}
				k, v := split[0], split[1]
				f, err := strconv.ParseFloat(v, 64)
				out.MaybeDie(err, "unable to parse add %q: %v", add, err)

				operations = append(operations, kadm.AlterClientQuotaOp{
					Key:   k,
					Value: f,
				})
			}
			for _, del := range deletes {
				operations = append(operations, kadm.AlterClientQuotaOp{
					Key:    del,
					Remove: true,
				})
			}

			request := []kadm.AlterClientQuotaEntry{{Entity: entity, Ops: operations}}
			var altered kadm.AlteredClientQuotas
			if dry {
				zap.L().Sugar().Debug("dry run: this result will not alter the client quotas")
				altered, err = adm.ValidateAlterClientQuotas(cmd.Context(), request)
			} else {
				altered, err = adm.AlterClientQuotas(cmd.Context(), request)
			}
			out.MaybeDie(err, "unable to run alter client quotas: %v", err)
			err = printAlteredQuotas(f, altered)
			out.MaybeDie(err, "unable to print altered quotas: %v", err)
		},
	}
	cmd.Flags().StringSliceVar(&names, "name", nil, "Entity for exact matching. Format type=name where type is client-id or client-id-prefix (repeatable)")
	cmd.Flags().StringSliceVar(&defaults, "default", nil, "Entity type for default matching, where type is client-id or client-id-prefix (repeatable)")
	cmd.Flags().StringSliceVar(&adds, "add", nil, "Key=value quota to add, where the value is a float number (repeatable)")
	cmd.Flags().StringSliceVar(&deletes, "delete", nil, "Key of the quota to delete (repeatable)")
	cmd.Flags().BoolVar(&dry, "dry", false, "Key of the quota to delete (repeatable)")

	cmd.MarkFlagsOneRequired("name", "default")
	cmd.MarkFlagsOneRequired("add", "delete")
	return cmd
}

func printAlteredQuotas(f config.OutFormatter, altered kadm.AlteredClientQuotas) error {
	// We only alter a single entity/quota.
	var resp createResponse
	for _, entry := range altered {
		entity, entityStr := parseEntityData(entry.Entity)
		status := "OK"
		if entry.Err != nil {
			status = fmt.Sprintf("Error: %v", entry.ErrMessage)
		}
		resp = createResponse{
			Entity:    entity,
			Status:    status,
			entityStr: entityStr,
		}
	}
	if isText, _, s, err := f.Format(resp); !isText {
		if err != nil {
			return fmt.Errorf("unable to print in the required format %q: %v", f.Kind, err)
		}
		fmt.Println(s)
		return nil
	}

	tw := out.NewTable("entity", "status")
	defer tw.Flush()
	tw.Print(resp.entityStr, resp.Status)

	return nil
}
