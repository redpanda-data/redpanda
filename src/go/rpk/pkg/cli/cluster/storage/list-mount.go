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

	dataplanev1alpha2 "buf.build/gen/go/redpandadata/dataplane/protocolbuffers/go/redpanda/api/dataplane/v1alpha2"
	"connectrpc.com/connect"
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

			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			config.CheckExitServerlessAdmin(p)

			var migrations []rpadmin.MigrationState
			if p.FromCloud {
				cl, err := createDataplaneClient(p)
				out.MaybeDieErr(err)

				resp, err := cl.CloudStorage.ListMountTasks(cmd.Context(), connect.NewRequest(&dataplanev1alpha2.ListMountTasksRequest{}))
				out.MaybeDie(err, "unable to list mount/unmount operations: %v", err)

				if resp != nil {
					migrations = listMountTaskToAdminMigrationState(resp.Msg)
				}
			} else {
				adm, err := adminapi.NewClient(cmd.Context(), fs, p)
				out.MaybeDie(err, "unable to initialize admin client: %v", err)

				migrations, err = adm.ListMigrations(cmd.Context())
				out.MaybeDie(err, "unable to list migrations: %v", err)
			}
			printDetailedListMount(f, filterOptFromString(filter), rpadminMigrationStateToMigrationState(migrations), os.Stdout)
		},
	}
	p.InstallFormatFlag(cmd)
	cmd.Flags().StringVarP(&filter, "filter", "f", "all", "Filter the list of migrations by state. Only valid for text")
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
	switch strings.ToLower(s) {
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
func rpadminTopicsToStringSlice(in []rpadmin.NamespacedOrInboundTopic) (resp []string) {
	for _, entry := range in {
		if entry.Namespace != nil {
			resp = append(resp, fmt.Sprintf("%s/%s", *entry.Namespace, entry.Topic))
		} else if entry.SourceTopicReference.Topic != "" {
			resp = append(resp, entry.SourceTopicReference.Topic)
		} else {
			resp = append(resp, entry.Topic)
		}
	}
	return
}

type migrationState struct {
	ID            int      `json:"id" yaml:"id"`
	State         string   `json:"state" yaml:"state"`
	MigrationType string   `json:"type" yaml:"type"`
	Topics        []string `json:"topics" yaml:"topics"`
}

func listMountTaskToAdminMigrationState(resp *dataplanev1alpha2.ListMountTasksResponse) []rpadmin.MigrationState {
	var migrations []rpadmin.MigrationState
	if resp != nil {
		for _, task := range resp.Tasks {
			if task != nil {
				migrations = append(migrations, rpadmin.MigrationState{
					ID:    int(task.Id),
					State: strings.TrimPrefix(task.State.String(), "STATE_"),
					Migration: rpadmin.Migration{
						MigrationType: task.Type.String(),
						Topics:        mountTaskTopicsToNamespacedOrInboundTopics(task.Topics, task.Type),
					},
				})
			}
		}
	}
	return migrations
}

// mountTaskTopicsToNamespacedOrInboundTopics converts the dataplane's
// mountTaskTopics to the rpadmin's equivalent.
func mountTaskTopicsToNamespacedOrInboundTopics(taskTopics []*dataplanev1alpha2.MountTask_Topic, taskType dataplanev1alpha2.MountTask_Type) []rpadmin.NamespacedOrInboundTopic {
	var topics []rpadmin.NamespacedOrInboundTopic
	for _, topic := range taskTopics {
		// Inbound == Mount.
		if taskType == dataplanev1alpha2.MountTask_TYPE_MOUNT {
			topics = append(topics, rpadmin.NamespacedOrInboundTopic{
				InboundTopic: rpadmin.InboundTopic{
					SourceTopicReference: rpadmin.NamespacedTopic{
						Topic: topic.SourceTopicReference, // The topic of the bucket you are mounting.
					},
				},
			})
		} else {
			topics = append(topics, rpadmin.NamespacedOrInboundTopic{
				NamespacedTopic: rpadmin.NamespacedTopic{
					Topic: topic.TopicReference, // The topic in the cluster you are un-mounting.
				},
			})
		}
	}
	return topics
}
