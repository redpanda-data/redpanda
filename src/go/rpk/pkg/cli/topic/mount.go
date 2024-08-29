// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package topic

import (
	"context"
	"strings"
	"time"

	"github.com/redpanda-data/common-go/rpadmin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newMountCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var (
		to       string
		location string
		resume   bool
		timeout  time.Duration
	)

	cmd := &cobra.Command{
		Use:   "mount [TOPIC]",
		Short: "Mount a topic to the Redpanda Cluster from Tiered Storage",
		Long: `Mount a topic from Tiered Storage to the Redpanda cluster.

This command mounts a topic in the Redpanda Cluster using log segments stored
in Tiered Storage.

Requirements:
- Tiered storage must be enabled.
- The topic must have a minimum of three partitions.
- Log segments for the topic must be available in Tiered Storage.
- A topic with the same name must not already exist in the cluster.`,
		Example: `
Mounts topic my-typic from Tiered Storage to the cluster in the my-namespace
	rpk topic mount my-topic

Mount topic my-topic from Tiered Storage to the cluster in the my-namespace 
with my-new-topic as the new topic name
	rpk topic mount my-namespace/my-topic --to my-namespace/my-new-topic
`,
		Args: cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, from []string) {
			pf, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			config.CheckExitCloudAdmin(pf)
			adm, err := adminapi.NewClient(fs, pf)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			n, t := nsTopic(from[0])
			if t == "" {
				out.Die("topic is required")
			}
			topic := rpadmin.InboundTopic{
				SourceTopic: rpadmin.Topic{
					Topic:     n,
					Namespace: t,
				},
			}
			an, at := nsTopic(to)
			if at != "" {
				topic.Alias = &rpadmin.Topic{
					Topic:     an,
					Namespace: at,
				}
			}

			if location != "" {
				topic.Location = location
			}

			migration, err := adm.AddInboundMigration(cmd.Context(), rpadmin.InboundMigration{
				MigrationType:  "inbound",
				Topics:         []rpadmin.InboundTopic{topic},
				ConsumerGroups: []string{}, // not implemented yet
			})
			out.MaybeDie(err, "unable to request topic migration: %v", err)

			ctx := cmd.Context()
			if timeout > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, timeout)
				defer cancel()
			}

			if resume {
				migrations, err := adm.ListMigrations(ctx)
				out.MaybeDie(err, "unable to list migrations: %v", err)
				id, err := getMigrationIdByName(t, migrations)
				if err != nil {
					return
				}

				ms, err := adm.GetMigration(ctx, id)
				out.MaybeDie(err, "unable to get migration state: %v", err)

				status, err := rpadmin.MigrationStatusFromString(ms.State)
				out.MaybeDie(err, "unable to parse migration status: %v", err)
				err = resumeMigrationByStatus(ctx, id, adm, status)
				out.MaybeDie(err, "unable to resume migration: %v", err)
				out.Die("migration resumed and completed successfully")
			}

			// TODO add more words to tell the user current state
			err = checkMigrationActionAndAdvanceState(ctx, migration.ID, adm, rpadmin.MigrationActionPrepare, rpadmin.MigrationStatusPrepared, timeout)
			out.MaybeDie(err, "unable to prepare migration %v", err)

			err = checkMigrationActionAndAdvanceState(ctx, migration.ID, adm, rpadmin.MigrationActionExecute, rpadmin.MigrationStatusExecuted, timeout)
			out.MaybeDie(err, "unable to execute migration %v", err)

			err = checkMigrationActionAndAdvanceState(ctx, migration.ID, adm, rpadmin.MigrationActionFinish, rpadmin.MigrationStatusFinished, timeout)
			out.MaybeDie(err, "unable to finish migration %v", err)
		},
	}
	cmd.Flags().StringVar(&to, "to", "", "New namespace/topic name for the mounted topic (optional)")
	cmd.Flags().StringVarP(&location, "location", "l", "", "Location (optional)") // need more info on this
	cmd.Flags().DurationVar(&timeout, "timeout", 0, "Timeout for the migration to finish (optional)")
	cmd.Flags().BoolVar(&resume, "resume", false, "Resume allows resuming an in progress unmount. Parameters must be the same as the original unmount")
	return cmd
}

// nsTopic splits a topic string consisting of <namespace>/<topicName> and
// returns each component, if the namespace is not specified, returns 'kafka'.
func nsTopic(nst string) (namespace string, topic string) {
	nsTopic := strings.SplitN(nst, "/", 2)
	if len(nsTopic) == 1 {
		namespace = "kafka"
		topic = nsTopic[0]
	} else {
		namespace = nsTopic[0]
		topic = nsTopic[1]
	}
	return namespace, topic
}
