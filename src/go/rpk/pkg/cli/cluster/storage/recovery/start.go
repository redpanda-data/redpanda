// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package recovery

import (
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

func newStartCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var (
		wait            bool
		pollingInterval time.Duration
	)

	cmd := &cobra.Command{
		Use:   "start",
		Short: "Start the topic restoration process",
		Long: `Start the topic restoration process.
		
This command starts the process of restoring topics from the archival bucket.
If the wait flag (--wait/-w) is set, the command will poll the status of the
recovery process until it's finished.`,
		Args: cobra.NoArgs,
		Run: func(cmd *cobra.Command, _ []string) {
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			config.CheckExitCloudAdmin(p)

			client, err := adminapi.NewClient(cmd.Context(), fs, p)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			ctx := cmd.Context()

			_, err = client.StartAutomatedRecovery(ctx)
			var he *rpadmin.HTTPResponseError
			if errors.As(err, &he) {
				if he.Response.StatusCode == 404 {
					body, bodyErr := he.DecodeGenericErrorBody()
					if bodyErr == nil {
						out.Die("Not found: %s", body.Message)
					}
				} else if he.Response.StatusCode == 400 {
					body, bodyErr := he.DecodeGenericErrorBody()
					if bodyErr == nil {
						out.Die("Cannot start topic recovery: %s", body.Message)
					}
				}
			}

			out.MaybeDie(err, "error starting topic recovery: %v", err)
			fmt.Println("Successfully started topic recovery")

			if !wait {
				fmt.Println("To check the recovery status, run 'rpk cluster storage restore status'")
				return
			}

			fmt.Println("Waiting for topic recovery to complete...")

			for {
				status, err := client.PollAutomatedRecoveryStatus(ctx)
				out.MaybeDie(err, "failed to poll automated recovery status: %v", err)

				pending := false
				if status.State != "inactive" {
					pending = true
				} else {
					for _, topicDownload := range status.TopicDownloads {
						if topicDownload.PendingDownloads > 0 {
							pending = true
							break
						}
					}
				}

				if !pending {
					failedPartitionReplicas := []string{}
					for _, topicDownload := range status.TopicDownloads {
						if topicDownload.FailedDownloads > 0 {
							failedPartitionReplicas = append(failedPartitionReplicas, topicDownload.TopicNamespace)
						}
					}

					if len(failedPartitionReplicas) > 0 {
						out.Die("automated recovery failed to download partition replicas: %v", failedPartitionReplicas)
					}

					break
				}
				time.Sleep(pollingInterval)
			}

			fmt.Println("Topic recovery completed successfully.")
		},
	}

	cmd.Flags().MarkDeprecated("topic-name-pattern", "Not supported")
	cmd.Flags().BoolVarP(&wait, "wait", "w", false, "Wait until auto-restore is complete")
	cmd.Flags().DurationVar(&pollingInterval, "polling-interval", 5*time.Second, "The status check interval (e.g. '30s', '1.5m'); ignored if --wait is not used")

	return cmd
}
