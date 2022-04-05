// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// Package brokers contains commands to talk to the Redpanda's admin brokers
// endpoints.
package brokers

import (
	"context"
	"fmt"
	"strconv"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/api/admin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/kafka"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
)

// NewCommand returns the brokers admin command.
func NewCommand(fs afero.Fs) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "brokers",
		Short: "View and configure Redpanda brokers through the admin listener.",
		Args:  cobra.ExactArgs(0),
	}
	cmd.AddCommand(
		newListCommand(fs),
		newListPartitionsCommand(fs),
		newDecommissionBroker(fs),
		newRecommissionBroker(fs),
	)
	return cmd
}

func newListCommand(fs afero.Fs) *cobra.Command {
        return &cobra.Command{
                Use:     "list",
                Aliases: []string{"ls"},
                Short:   "List the brokers in your cluster.",
                Args:    cobra.ExactArgs(0),
                Run: func(cmd *cobra.Command, _ []string) {
                        p := config.ParamsFromCommand(cmd)
                        cfg, err := p.Load(fs)
                        out.MaybeDie(err, "unable to load config: %v", err)

                        cl, err := admin.NewClient(fs, cfg)
                        out.MaybeDie(err, "unable to initialize admin client: %v", err)

                        bs, err := cl.Brokers()
                        out.MaybeDie(err, "unable to request brokers: %v", err)

                        tw := out.NewTable("Node ID", "Num Cores", "Membership Status")
                        defer tw.Flush()
                        for _, b := range bs {
                                tw.Print(b.NodeID, b.NumCores, b.MembershipStatus)
                        }
                },
        }
}

func newListPartitionsCommand(fs afero.Fs) *cobra.Command {
	var (
		brokerId string
	)
	cmd :=  &cobra.Command{
		Use:     "list-partitions",
		Aliases: []string{"ls-ps"},
		Short:   "List the partitions in a broker in your cluster.",
		Args:    cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			p := config.ParamsFromCommand(cmd)
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			//cl, err := admin.NewClient(fs, cfg)
			//out.MaybeDie(err, "unable to initialize admin client: %v", err)

			adm, err := kafka.NewAdmin(fs, p, cfg)
                        out.MaybeDie(err, "unable to initialize kafka client: %v", err)
                        defer adm.Close()

			var m kadm.Metadata
                        m, err = adm.Metadata(context.Background(), args...)
                        out.MaybeDie(err, "unable to request metadata: %v", err)

			tw := out.NewTable("TOPIC", "PARTITION", "IS_LEADER")
			defer tw.Flush()

			brokerIdInt, err := strconv.Atoi(brokerId)
			out.MaybeDie(err, "unable to parse brokerId: %v", err)

			for _, t := range m.Topics.Sorted() {
				for _, pt := range t.Partitions.Sorted() {
					for _, rs := range pt.Replicas {
						if int(rs) == brokerIdInt {
							var isLeader = "NO"
							if int(pt.Leader) == brokerIdInt {
								isLeader = "YES"
							}
							tw.Print(t.Topic , pt.Partition, isLeader)
						}
					}
				}
			}
		},
	}

	cmd.Flags().StringVar(&brokerId, "broker-id", "b", "print the partitions on broker id")

	return cmd
}

func newDecommissionBroker(fs afero.Fs) *cobra.Command {
	return &cobra.Command{
		Use:   "decommission [BROKER ID]",
		Short: "Decommission the given broker.",
		Long: `Decommission the given broker.

Decommissioning a broker removes it from the cluster.

A decommission request is sent to every broker in the cluster, only the cluster
leader handles the request.
`,
		Args: cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			broker, err := strconv.Atoi(args[0])
			out.MaybeDie(err, "invalid broker %s: %v", args[0], err)
			if broker < 0 {
				out.Die("invalid negative broker id %v", broker)
			}

			p := config.ParamsFromCommand(cmd)
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			cl, err := admin.NewClient(fs, cfg)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			err = cl.DecommissionBroker(broker)
			out.MaybeDie(err, "unable to decommission broker: %v", err)

			fmt.Printf("Success, broker %d has been decommissioned!\n", broker)
		},
	}
}

func newRecommissionBroker(fs afero.Fs) *cobra.Command {
	return &cobra.Command{
		Use:   "recommission [BROKER ID]",
		Short: "Recommission the given broker if it is still decommissioning.",
		Long: `Recommission the given broker if is is still decommissioning.

Recommissioning can stop an active decommission.

Once a broker is decommissioned, it cannot be recommissioned through this
command.

A recommission request is sent to every broker in the cluster, only
the cluster leader handles the request.

`,
		Args: cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			broker, err := strconv.Atoi(args[0])
			out.MaybeDie(err, "invalid broker %s: %v", args[0], err)
			if broker < 0 {
				out.Die("invalid negative broker id %v", broker)
			}

			p := config.ParamsFromCommand(cmd)
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			cl, err := admin.NewClient(fs, cfg)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			err = cl.RecommissionBroker(broker)
			out.MaybeDie(err, "unable to recommission broker: %v", err)

			fmt.Printf("Success, broker %d has been recommissioned!\n", broker)
		},
	}
}
