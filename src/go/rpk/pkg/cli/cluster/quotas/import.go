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
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/kafka"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/types"
	"gopkg.in/yaml.v3"
)

// quotasDiff represents a delta in the quotas after importing a quota.
type quotasDiff struct {
	EntityStr string `json:"entity,omitempty" yaml:"entity,omitempty"`
	QuotaType string `json:"quota-type,omitempty" yaml:"quota-type,omitempty"`
	OldValue  string `json:"old-value,omitempty" yaml:"old-value,omitempty"`
	NewValue  string `json:"new-value,omitempty" yaml:"new-value,omitempty"`
}

func importCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var (
		from      string
		noConfirm bool
	)
	cmd := &cobra.Command{
		Use:   "import",
		Short: "Import client quotas",
		Long: `Import client quotas.

Use this command to import client quotas in the format produced by 
'rpk cluster quotas describe --format json/yaml'.

The schema of the import string matches the schema from
'rpk cluster quotas describe --format help':

{
  quotas: []{
    entity: []{
      name: string
      type: string
    }
    values: []{
      key: string
      values: string
    }
  }
}

Use the '--no-confirm' flag if you wish to avoid the confirmation prompt.
`,
		Example: `
Import client quotas from a file:
  rpk cluster quotas import --from /path/to/file

Import client quotas from a string:
  rpk cluster quotas import --from '{"quotas":...}'
`,
		Args: cobra.NoArgs,
		Run: func(cmd *cobra.Command, _ []string) {
			f := p.Formatter
			if h, ok := f.Help([]quotasDiff{}); ok {
				out.Exit(h)
			}

			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)

			adm, err := kafka.NewAdmin(fs, p)
			out.MaybeDie(err, "unable to initialize kafka client: %v", err)
			defer adm.Close()

			var quotas describeResponse
			var source []byte
			// --from flag accepts either a file, or a string, we try first
			// to read as a file as is the most expected usage.
			file, err := afero.ReadFile(fs, from)
			if err == nil {
				source = file
			} else {
				if os.IsNotExist(err) || strings.Contains(err.Error(), "file name too long") {
					source = []byte(from)
				} else {
					out.Exit("unable to read file: %v", err)
				}
			}
			if err = json.Unmarshal(source, &quotas); err != nil {
				yamlErr := yaml.Unmarshal(source, &quotas)
				out.MaybeDie(yamlErr, "unable to parse quotas from %q: %v: %v", from, err, yamlErr)
			}

			importedQuotas, err := responseToDescribed(quotas)
			out.MaybeDie(err, "unable to parse quotas: %v", err)

			// Describe all quotas.
			currentQuotas, err := adm.DescribeClientQuotas(cmd.Context(), false, []kadm.DescribeClientQuotaComponent{})
			out.MaybeDie(err, "unable to describe client quotas: %v", err)

			diff := calculateQuotasDiff(currentQuotas, importedQuotas)
			if len(diff) == 0 {
				out.Exit("No changes detected from import")
			}
			printDiff(f, diff)

			if !noConfirm {
				ok, err := out.Confirm("Confirm client quotas import above?")
				out.MaybeDie(err, "unable to confirm deletion: %v", err)
				if !ok {
					out.Exit("Import canceled.")
				}
			}
			_, err = adm.AlterClientQuotas(cmd.Context(), describedToAlterEntry(currentQuotas, importedQuotas))
			out.MaybeDie(err, "unable to alter quotas: %v", err)

			if f.Kind == "text" {
				fmt.Println("Successfully imported the client quotas")
			}
		},
	}

	cmd.Flags().StringVar(&from, "from", "", "Either the quotas or a path to a file containing the quotas to import; check help text for more information")
	cmd.Flags().BoolVar(&noConfirm, "no-confirm", false, "Disable confirmation prompt")

	cmd.MarkFlagRequired("from")
	return cmd
}

// responseToDescribed converts the describeResponse quota (imported source) to
// a []kadm.DescribedClientQuota.
func responseToDescribed(quotas describeResponse) ([]kadm.DescribedClientQuota, error) {
	var resp []kadm.DescribedClientQuota
	for _, q := range quotas.DescribedQuotas {
		var (
			entity []kadm.ClientQuotaEntityComponent
			values []kadm.ClientQuotaValue
		)
		for _, e := range q.Entity {
			e := e
			entity = append(entity, kadm.ClientQuotaEntityComponent{
				Type: e.Type,
				Name: &e.Name,
			})
		}
		for _, v := range q.Values {
			floatVal, err := strconv.ParseFloat(v.Value, 64)
			if err != nil {
				return nil, fmt.Errorf("unable to parse client quota value %q: %v", v.Value, err)
			}
			values = append(values, kadm.ClientQuotaValue{
				Key:   v.Key,
				Value: floatVal,
			})
		}
		resp = append(resp, kadm.DescribedClientQuota{
			Entity: entity,
			Values: values,
		})
	}
	return resp, nil
}

// describedToAlterEntry creates a []kadm.AlterClientQuotaEntry based on the
// toDelete and toAdd described client quotas. The entry can be used to issue
// an alter client quota request.
func describedToAlterEntry(toDelete, toAdd []kadm.DescribedClientQuota) []kadm.AlterClientQuotaEntry {
	var entries []kadm.AlterClientQuotaEntry
	addEntries := func(described []kadm.DescribedClientQuota, delete bool) {
		for _, d := range described {
			var (
				entity     []kadm.ClientQuotaEntityComponent
				operations []kadm.AlterClientQuotaOp
			)
			for _, e := range d.Entity {
				entity = append(entity, kadm.ClientQuotaEntityComponent{
					Type: e.Type,
					Name: e.Name,
				})
			}
			for _, o := range d.Values {
				if delete {
					operations = append(operations, kadm.AlterClientQuotaOp{
						Key:    o.Key,
						Remove: true,
					})
				} else {
					operations = append(operations, kadm.AlterClientQuotaOp{
						Key:   o.Key,
						Value: o.Value,
					})
				}
			}
			entries = append(entries, kadm.AlterClientQuotaEntry{
				Entity: entity,
				Ops:    operations,
			})
		}
	}
	addEntries(toDelete, true)
	addEntries(toAdd, false)
	return entries
}

// calculateQuotasDiff calculates the diff between 'before' and 'after', any
// value that is not present will be marked as '-' in the result string.
func calculateQuotasDiff(before, after []kadm.DescribedClientQuota) []quotasDiff {
	type delta struct {
		oldValue string
		newValue string
	}
	type quotaTypeMap map[string]delta

	entityMap := make(map[string]quotaTypeMap)

	// Fill the map with old values.
	for _, q := range before {
		e := q.Entity
		types.Sort(e)
		_, entityStr := parseEntityData(e)
		qMap := entityMap[entityStr]
		if qMap == nil {
			qMap = quotaTypeMap{}
			entityMap[entityStr] = qMap
		}
		for _, v := range q.Values {
			qMap[v.Key] = delta{
				oldValue: strconv.FormatFloat(v.Value, 'f', -1, 64),
			}
		}
	}

	// Update map with new values and track differences.
	for _, q := range after {
		e := q.Entity
		types.Sort(e)
		_, entityStr := parseEntityData(e)
		qMap := entityMap[entityStr]
		if qMap == nil {
			qMap = quotaTypeMap{}
			entityMap[entityStr] = qMap
		}
		for _, v := range q.Values {
			newVal := strconv.FormatFloat(v.Value, 'f', -1, 64)
			if d, exists := qMap[v.Key]; exists {
				// If the value changed, we add it to the map.
				if newVal != d.oldValue {
					qMap[v.Key] = delta{oldValue: d.oldValue, newValue: newVal}
				} else {
					// If not, we remove it, we do nothing if the values are
					// the same.
					delete(qMap, v.Key)
				}
			} else {
				qMap[v.Key] = delta{oldValue: "-", newValue: newVal}
			}
		}
	}

	// Prepare the result
	var diffResult []quotasDiff
	for entityStr, qMap := range entityMap {
		for key, d := range qMap {
			newValue := d.newValue
			if newValue == "" {
				newValue = "-"
			}
			diffResult = append(diffResult, quotasDiff{
				EntityStr: entityStr,
				QuotaType: key,
				OldValue:  d.oldValue,
				NewValue:  newValue,
			})
		}
	}
	return diffResult
}

func printDiff(f config.OutFormatter, diff []quotasDiff) {
	if isText, _, formatted, err := f.Format(diff); !isText {
		out.MaybeDie(err, "unable to print in the required format %q: %v", f.Kind, err)
		fmt.Println(formatted)
		return
	}
	tw := out.NewTable("ENTITY", "QUOTA-TYPE", "OLD-VALUE", "NEW-VALUE")
	defer tw.Flush()

	for _, d := range diff {
		tw.PrintStructFields(d)
	}
}
