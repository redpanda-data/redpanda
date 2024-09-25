package brokers

import (
	"fmt"
	"strconv"

	"github.com/redpanda-data/common-go/rpadmin"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/redpanda"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newDecommissionBroker(fs afero.Fs, p *config.Params) *cobra.Command {
	var force bool
	cmd := &cobra.Command{
		Use:   "decommission [BROKER ID]",
		Short: "Decommission the given broker",
		Long: `Decommission the given broker.

Decommissioning a broker removes it from the cluster.

A decommission request is sent to every broker in the cluster, only the cluster
leader handles the request.

For safety on v22.x clusters, this command will not run if the requested 
broker is in maintenance mode. As of v23.x, Redpanda supports 
decommissioning a node that is currently in maintenance mode. If you are on 
a v22.x cluster and need to bypass the maintenance mode check (perhaps your 
cluster is unreachable), use the hidden --force flag.
`,
		Args: cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			broker, err := strconv.Atoi(args[0])
			out.MaybeDie(err, "invalid broker %s: %v", args[0], err)
			if broker < 0 {
				out.Die("invalid negative broker id %v", broker)
			}

			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			config.CheckExitCloudAdmin(p)

			cl, err := adminapi.NewClient(cmd.Context(), fs, p)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			if !force {
				brokers, err := cl.Brokers(cmd.Context())
				out.MaybeDie(err, "unable to get broker list: %v; to bypass the node version check re-run this with --force; see this command's help text for more details", err)

				var (
					b             rpadmin.Broker
					found, anyOld bool
				)
				for _, br := range brokers {
					if br.NodeID == broker {
						if br.Version == "" {
							out.Exit("version for broker %d is unknown, is the node offline?\nto bypass the node version check re-run this with --force; see this command's help text for more details", br.NodeID)
						}
						version, err := redpanda.VersionFromString(br.Version)
						out.MaybeDie(err, "unable to get broker %d version: %v; to bypass the node version check re-run this with --force; see this command's help text for more details", br.NodeID, err)
						isOld := version.Less(redpanda.Version{Major: 23, Feature: 1, Patch: 1})

						anyOld = anyOld || isOld
						b, found = br, true
						break
					}
				}
				if !found {
					out.Die("unable to find broker %v in the cluster; to bypass the node version check re-run this with --force; see this command's help text for more details", broker)
				}

				// If any of the brokers is older than v23.1.1 we need to check
				// that the broker that is about to be decommissioned is not
				// in maintenance mode.
				if anyOld {
					// Old brokers (< v22.1) don't have maintenance mode, so we must
					// check if b.Maintenance is not nil.
					if b.Maintenance != nil && b.Maintenance.Draining {
						out.Die(`Node cannot be decommissioned while it is in maintenance mode.
Take the node out of maintenance mode first by running: 
    rpk cluster maintenance disable %v
To bypass the node version check re-run this with --force; see this command's
help text for more details on why.`, broker)
					}
				}
			}

			err = cl.DecommissionBroker(cmd.Context(), broker)
			out.MaybeDie(err, "unable to decommission broker: %v", err)

			fmt.Printf("Success, broker %d decommission started.  Use `rpk redpanda admin brokers decommission-status %d` to monitor data movement.\n`", broker, broker)
		},
	}

	// Before using the flag, make sure that the controller leader version is at
	// least 23.1.0, using it is the equivalent of manually issuing the request
	// using /v1/brokers/<broker-id>/decommission.
	cmd.Flags().BoolVar(&force, "force", false, "If enabled, rpk will issue the decommission request without checking if the broker is in maintenance mode")
	cmd.Flags().MarkHidden("force")

	return cmd
}
