// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package storage

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/redpanda-data/common-go/rpadmin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newMountList(fs afero.Fs, p *config.Params) *cobra.Command {
	var filter string
	cmd := &cobra.Command{
		Use:     "list-mount",
		Short:   "List mount/unmount operations",
		Aliases: []string{"list-unmount"},
		Long: `List mount/unmount operations on a topic to the Redpanda cluster from Tiered Storage.

You can also filter the list by state using the --filter flag. The possible states are:
- planned
- prepared
- executed
- finished

If no filter is provided, all migrations will be listed.`,
		Example: `
Lists mount/unmount operations
	rpk cluster storage list-mount

Use filter to list only migrations in a specific state
	rpk cluster storage list-mount --filter planned
`,
		Args: cobra.NoArgs,
		Run: func(cmd *cobra.Command, _ []string) {
			f := p.Formatter
			if h, ok := f.Help([]migrationState{}); ok {
				out.Exit(h)
			}

			pf, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			config.CheckExitCloudAdmin(pf)
			adm, err := adminapi.NewClient(cmd.Context(), fs, pf)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			migrations, err := adm.ListMigrations(cmd.Context())
			out.MaybeDie(err, "unable to list migrations: %v", err)
			printDetailedListMount(p.Formatter, filterOptFromString(filter), rpadminMigrationStateToMigrationState(migrations), os.Stdout)
		},
	}
	p.InstallFormatFlag(cmd)
	cmd.Flags().StringVarP(&filter, "filter", "f", "", "Filter the list of migrations by state. Only valid for text")
	return cmd
}

type filterOpts int

const (
	FilterOptsAll filterOpts = iota
	FilterOptsPlanned
	FilterOptsPrepared
	FilterOptsExecuted
	FilterOptsFinished
)

func (f filterOpts) String() string {
	switch f {
	case FilterOptsAll:
		return "all"
	case FilterOptsPlanned:
		return "planned"
	case FilterOptsPrepared:
		return "prepared"
	case FilterOptsExecuted:
		return "executed"
	case FilterOptsFinished:
		return "finished"
	default:
		return ""
	}
}

func filterOptFromString(s string) filterOpts {
	switch s {
	case "planned":
		return FilterOptsPlanned
	case "prepared":
		return FilterOptsPrepared
	case "executed":
		return FilterOptsExecuted
	case "finished":
		return FilterOptsFinished
	case "all":
		return FilterOptsAll
	default:
		return -1
	}
}

func printDetailedListMount(f config.OutFormatter, ft filterOpts, d []migrationState, w io.Writer) {
	if isText, _, t, err := f.Format(d); !isText {
		out.MaybeDie(err, "unable to print in the requested format %q: %v", f.Kind, err)
		fmt.Fprintln(w, t)
		return
	}
	tw := out.NewTableTo(w, "ID", "State", "Migration", "Topics")
	defer tw.Flush()
	for _, m := range d {
		if ft != FilterOptsAll {
			if m.State != ft.String() {
				continue
			}
		}
		tw.Print(m.ID, m.State, m.MigrationType, strings.Join(m.Topics, ", "))
	}
}

func rpadminMigrationStateToMigrationState(in []rpadmin.MigrationState) (resp []migrationState) {
	resp = make([]migrationState, 0, len(in))
	for _, entry := range in {
		resp = append(resp, migrationState{
			ID:            entry.ID,
			State:         entry.State,
			MigrationType: entry.Migration.MigrationType,
			Topics:        rpadminTopicsToStringSlice(entry.Migration.Topics),
		})
	}
	return resp
}

// rpadminTopicsToStringSlice converts a slice of rpadmin.NamespacedTopic to a slice of strings
// if the topic has a non nil namespace it will appear as `namespace:topic`
// otherwise it will appear as `topic`.
func rpadminTopicsToStringSlice(in []rpadmin.NamespacedTopic) (resp []string) {
	for _, entry := range in {
		if entry.Namespace != nil {
			resp = append(resp, fmt.Sprintf("%s/%s", *entry.Namespace, entry.Topic))
			continue
		}
		resp = append(resp, entry.Topic)
	}
	return
}

type migrationState struct {
	ID            int      `json:"id" yaml:"id"`
	State         string   `json:"state" yaml:"state"`
	MigrationType string   `json:"type" yaml:"type"`
	Topics        []string `json:"topics" yaml:"topics"`
}
