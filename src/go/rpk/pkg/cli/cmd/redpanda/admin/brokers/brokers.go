// Copyright 2021 Vectorized, Inc.
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
	"crypto/tls"
	"fmt"
	"strconv"

	"github.com/spf13/cobra"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/api/admin"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/out"
)

// NewCommand returns the brokers admin command.
func NewCommand(
	hostsClosure func() []string, tlsClosure func() (*tls.Config, error),
) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "brokers",
		Short: "View and configure Redpanda brokers through the admin listener.",
		Args:  cobra.ExactArgs(0),
	}
	closures := closures{hostsClosure, tlsClosure}
	cmd.AddCommand(
		newListCommand(closures),
		newDecommissionBroker(closures),
		newRecommissionBroker(closures),
	)
	return cmd
}

type closures struct {
	hosts func() []string
	tls   func() (*tls.Config, error)
}

func (c closures) eval() ([]string, *tls.Config, error) {
	hosts := c.hosts()
	tls, err := c.tls()
	return hosts, tls, err
}

func newListCommand(closures closures) *cobra.Command {
	return &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List the brokers in your cluster.",
		Args:    cobra.ExactArgs(0),
		Run: func(*cobra.Command, []string) {
			hosts, tls, err := closures.eval()
			out.MaybeDie(err, "unable to load configuration: %v", err)

			cl, err := admin.NewAdminAPI(hosts, tls)
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

func newDecommissionBroker(closures closures) *cobra.Command {
	return &cobra.Command{
		Use:   "decommission [BROKER ID]",
		Short: "Decommission the given broker.",
		Long: `Decommission the given broker.

Decommissioning a broker removes it from the cluster.

A decommission request is sent to every broker in the cluster, only the cluster
leader handles the request.
`,
		Args: cobra.ExactArgs(1),
		Run: func(_ *cobra.Command, args []string) {
			broker, err := strconv.Atoi(args[0])
			out.MaybeDie(err, "invalid broker %s: %v", args[0], err)
			if broker < 0 {
				out.Die("invalid negative broker id %v", broker)
			}

			hosts, tls, err := closures.eval()
			out.MaybeDie(err, "unable to load configuration: %v", err)

			cl, err := admin.NewAdminAPI(hosts, tls)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			err = cl.DecommissionBroker(broker)
			out.MaybeDie(err, "unable to decommission broker: %v", err)

			fmt.Printf("Success, broker %d has been decommissioned!\n", broker)
		},
	}
}

func newRecommissionBroker(closures closures) *cobra.Command {
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
		Run: func(_ *cobra.Command, args []string) {
			broker, err := strconv.Atoi(args[0])
			out.MaybeDie(err, "invalid broker %s: %v", args[0], err)
			if broker < 0 {
				out.Die("invalid negative broker id %v", broker)
			}

			hosts, tls, err := closures.eval()
			out.MaybeDie(err, "unable to load configuration: %v", err)

			cl, err := admin.NewAdminAPI(hosts, tls)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			err = cl.RecommissionBroker(broker)
			out.MaybeDie(err, "unable to recommission broker: %v", err)

			fmt.Printf("Success, broker %d has been recommissioned!\n", broker)
		},
	}
}
