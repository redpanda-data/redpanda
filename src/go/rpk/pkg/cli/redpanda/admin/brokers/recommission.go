package brokers

import (
	"fmt"
	"strconv"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/api/admin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newRecommissionBroker(fs afero.Fs, p *config.Params) *cobra.Command {
	return &cobra.Command{
		Use:   "recommission [BROKER ID]",
		Short: "Recommission the given broker if it is still decommissioning",
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

			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			cl, err := admin.NewClient(fs, cfg)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			err = cl.RecommissionBroker(cmd.Context(), broker)
			out.MaybeDie(err, "unable to recommission broker: %v", err)

			fmt.Printf("Success, broker %d has been recommissioned!\n", broker)
		},
	}
}
