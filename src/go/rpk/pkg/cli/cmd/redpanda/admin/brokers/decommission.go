package brokers

import (
	"fmt"
	"strconv"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/api/admin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/redpanda"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newDecommissionBroker(fs afero.Fs) *cobra.Command {
	return &cobra.Command{
		Use:   "decommission [BROKER ID]",
		Short: "Decommission the given broker",
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

			brokers, err := cl.Brokers(cmd.Context())
			out.MaybeDie(err, "unable to get broker list: %v", err)

			var b admin.Broker
			var found bool
			for _, br := range brokers {
				if br.NodeID == broker {
					b, found = br, true
					break
				}
			}
			if !found {
				out.Die("unable to find broker %v in the cluster", broker)
			}

			version, err := redpanda.VersionFromString(b.Version)
			out.MaybeDie(err, "unable to get broker version: %v", err)

			if version.Less(redpanda.Version{Year: 23, Feature: 1, Patch: 1}) {
				// Old brokers (< v22.1) don't have maintenance mode, so we must
				// check if b.Maintenance is not nil.
				if b.Maintenance != nil && b.Maintenance.Draining {
					out.Die(`Node cannot be decommissioned while it is in maintenance mode.
Take the node out of maintenance mode first by running: 
    rpk cluster maintenance disable %v`, broker)
				}
			}

			err = cl.DecommissionBroker(cmd.Context(), broker)
			out.MaybeDie(err, "unable to decommission broker: %v", err)

			fmt.Printf("Success, broker %d has been decommissioned!\n", broker)
		},
	}
}
