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
	"errors"
	"fmt"
	"time"

	"github.com/redpanda-data/common-go/rpadmin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newUnmountCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var resume bool
	var timeout time.Duration

	cmd := &cobra.Command{
		Use:   "unmount [TOPIC]",
		Short: "Unmount a topic from the Redpanda Cluster",
		Long: `Unmount a topic from the Redpanda cluster and secure it in Tiered
Storage.

This command performs an operation that:
1. Rejects all writes to the topic
2. Flushes data to Tiered Storage
3. Removes the topic from the cluster

The unmount process ensures data safety and cluster integrity. Every
acknowledged message before the initiation of unmount is guaranteed to be
persisted in Tiered Storage, subject to Redpanda's existing data safety
guarantees. 

Key Points:
- During unmounting, any attempted writes or reads will receive an
  UNKNOWN_TOPIC_OR_PARTITION error.
- The unmount operation works independently of other topic configurations like
  remote.delete=false.
- After unmounting, the topic can be remounted to this cluster or a different
  cluster if the log segments are moved to that cluster's Tiered Storage.
`,
		Example: `Unmount topic 'my-topic' from the cluster in the 'my-namespace'
  rpk topic unmount my-namespace/my-topic
`,
		Args: cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, topics []string) {
			pf, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			config.CheckExitCloudAdmin(pf)
			adm, err := adminapi.NewClient(fs, pf)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			n, t := nsTopic(topics[0])
			if t == "" {
				out.Die("topic is required")
			}

			resp, err := adm.AddOutboundMigration(cmd.Context(), rpadmin.OutboundMigration{
				MigrationType: "outbound",
				Topics: []rpadmin.Topic{
					{
						Topic:     t,
						Namespace: n,
					},
				},
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
			err = checkMigrationActionAndAdvanceState(ctx, resp.ID, adm, rpadmin.MigrationActionPrepare, rpadmin.MigrationStatusPrepared, timeout)
			out.MaybeDie(err, "unable to prepare migration %v", err)
			err = checkMigrationActionAndAdvanceState(ctx, resp.ID, adm, rpadmin.MigrationActionExecute, rpadmin.MigrationStatusExecuted, timeout)
			out.MaybeDie(err, "unable to execute migration %v", err)
			err = checkMigrationActionAndAdvanceState(ctx, resp.ID, adm, rpadmin.MigrationActionFinish, rpadmin.MigrationStatusFinished, timeout)
			out.MaybeDie(err, "unable to finish migration %v", err)
		},
	}
	cmd.Flags().DurationVar(&timeout, "timeout", 0, "Timeout for the unmount process to finish")
	cmd.Flags().BoolVar(&resume, "resume", false, "Resume allows resuming an in progress unmount. Parameters must be the same as the original unmount")
	return cmd
}

type migrationClient interface {
	ExecuteMigration(ctx context.Context, id int, action rpadmin.MigrationAction) error
	GetMigration(ctx context.Context, id int) (rpadmin.MigrationState, error)
}

func checkMigrationActionAndAdvanceState(ctx context.Context, id int, adm migrationClient, doingAction rpadmin.MigrationAction, desiredStatus rpadmin.MigrationStatus, timeout time.Duration) error {
	if err := adm.ExecuteMigration(ctx, id, doingAction); err != nil {
		return fmt.Errorf("unable to execute migration: %w", err)
	}

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m, err := adm.GetMigration(ctx, id)
			if err != nil {
				return fmt.Errorf("unable to get migration state: %w", err)
			}
			if m.State == desiredStatus.String() {
				return nil
			}
			fmt.Printf("Current migration state: %s\n", m.State)
		case <-ctx.Done():
			switch err := ctx.Err(); {
			case errors.Is(err, context.DeadlineExceeded):
				return fmt.Errorf("operation timed out: %w", ctx.Err())
			case errors.Is(err, context.Canceled):
				return fmt.Errorf("operation was canceled: %w", ctx.Err())
			default:
				return fmt.Errorf("operation interrupted: %w", ctx.Err())
			}
		}
	}
}

func getMigrationIdByName(name string, migrations []rpadmin.MigrationState) (int, error) {
	for _, m := range migrations {
		for _, t := range m.Migration.Topics {
			if t.Topic == name {
				return m.ID, nil
			}
		}
	}
	return 0, fmt.Errorf("migration not found for topic %s", name)
}

func resumeMigrationByStatus(ctx context.Context, id int, adm migrationClient, state rpadmin.MigrationStatus) error {
	switch state {
	case rpadmin.MigrationStatusPlanned:
		err := checkMigrationActionAndAdvanceState(ctx, id, adm, rpadmin.MigrationActionPrepare, rpadmin.MigrationStatusPrepared, 0)
		out.MaybeDie(err, "unable to prepare migration %v", err)
		err = checkMigrationActionAndAdvanceState(ctx, id, adm, rpadmin.MigrationActionExecute, rpadmin.MigrationStatusExecuted, 0)
		out.MaybeDie(err, "unable to execute migration %v", err)
		err = checkMigrationActionAndAdvanceState(ctx, id, adm, rpadmin.MigrationActionFinish, rpadmin.MigrationStatusFinished, 0)
		out.MaybeDie(err, "unable to finish migration %v", err)
	case rpadmin.MigrationStatusPrepared:
		err := checkMigrationActionAndAdvanceState(ctx, id, adm, rpadmin.MigrationActionExecute, rpadmin.MigrationStatusExecuted, 0)
		out.MaybeDie(err, "unable to execute migration %v", err)
		err = checkMigrationActionAndAdvanceState(ctx, id, adm, rpadmin.MigrationActionFinish, rpadmin.MigrationStatusFinished, 0)
		out.MaybeDie(err, "unable to finish migration %v", err)
	case rpadmin.MigrationStatusExecuted:
		err := checkMigrationActionAndAdvanceState(ctx, id, adm, rpadmin.MigrationActionFinish, rpadmin.MigrationStatusFinished, 0)
		out.MaybeDie(err, "unable to finish migration %v", err)
	case rpadmin.MigrationStatusFinished:
		out.Die("migration is already finished")
	default:
		out.Die("invalid migration status")
	}
	return nil
}
