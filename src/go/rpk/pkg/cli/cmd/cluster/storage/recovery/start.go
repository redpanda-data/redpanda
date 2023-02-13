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

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/api/admin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newStartCommand(fs afero.Fs) *cobra.Command {
	var topicNamePattern string

	cmd := &cobra.Command{
		Use:   "start",
		Short: "Start the topic recovery process",
		Long: `Start the topic recovery process.
		
This command starts the process of restoring topics from the archival bucket.`,
		Run: func(cmd *cobra.Command, args []string) {
			p := config.ParamsFromCommand(cmd)
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			client, err := admin.NewClient(fs, cfg)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			_, err = client.StartAutomatedRecovery(cmd.Context(), topicNamePattern)
			var he *admin.HTTPResponseError
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
		},
	}

	cmd.Flags().StringVar(&topicNamePattern, "topic-name-pattern", ".*", "A regex pattern to match topic names against. Only topics whose names match this pattern will be restored. If not passed, all topics will be restored.")

	return cmd
}
