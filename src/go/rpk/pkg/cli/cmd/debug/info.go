// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package debug

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/api"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/kafka"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/system"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
)

func NewInfoCommand(fs afero.Fs) *cobra.Command {
	var (
		configFile string
		send       bool
		timeout    time.Duration
	)
	cmd := &cobra.Command{
		Use:     "info",
		Short:   "Send usage stats to Redpanda Data.",
		Hidden:  true,
		Aliases: []string{"status"},
		Args:    cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, _ []string) {
			if !send {
				return
			}

			p := config.ParamsFromCommand(cmd)
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			if !cfg.Rpk.EnableUsageStats {
				log.Debug("Usage stats reporting is disabled. To enable, set rpk.enable_usage_stats true")
				return
			}

			cl, err := kafka.NewFranzClient(fs, p, cfg)
			out.MaybeDie(err, "unable to initialize kafka client: %v", err)
			defer cl.Close()

			var payload api.MetricsPayload

			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				if m, err := system.GatherMetrics(fs, timeout, *cfg); err != nil {
					fmt.Printf("Not sending system metrics, err: %v\n", err)
				} else {
					payload.FreeMemoryMB = m.FreeMemoryMB
					payload.FreeSpaceMB = m.FreeSpaceMB
					payload.CpuPercentage = m.CpuPercentage
				}
			}()

			wg.Add(1)
			go func() {
				defer wg.Done()
				ctx, cancel := context.WithTimeout(context.Background(), timeout)
				defer cancel()
				if ts, err := kadm.NewClient(cl).ListTopics(ctx); err != nil {
					fmt.Printf("Not sending topic metrics, err: %v\n", err)
				} else {
					numTopics := len(ts)
					var numPartitions int
					for _, t := range ts {
						numPartitions += len(t.Partitions)
					}
					payload.Topics = &numTopics
					payload.Partitions = &numPartitions
				}
			}()

			wg.Wait()

			api.SendMetrics(payload, *cfg)
		},
	}
	cmd.Flags().StringVar(&configFile, "config", "", "Redpanda config file, if not set the file will be searched for in the default locations")
	cmd.Flags().BoolVar(&send, "send", false, "If true, send resource usage data to Vectorzed.")
	cmd.Flags().DurationVar(&timeout, "timeout", 2*time.Second, "How long to wait to calculate the Redpanda CPU % utilization.")
	return cmd
}
