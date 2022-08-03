package partitions

import (
	"fmt"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/api/admin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func NewMovementCancelCommand(fs afero.Fs) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "movement-cancel",
		Short: "Cancel ongoing partitions reconfigurations",
		Long:  `Cancel all ongoing partitions reconfigurations happening in the cluster`,
		Args:  cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			p := config.ParamsFromCommand(cmd)
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			cl, err := admin.NewClient(fs, cfg)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			movements, err := cl.CancelAllPartitionsMovement(cmd.Context())
			out.MaybeDie(err, "unable to cancel partitions movements: %v", err)

			if len(movements) == 0 {
				fmt.Println("There are no partitions movements ongoing to cancel")
				return
			}
			printMovementsResult(movements)
		},
	}
	return cmd
}

func printMovementsResult(movements []admin.PartitionsMovementResult) {
	headers := []string{
		"NAMESPACE",
		"TOPIC",
		"PARTITION",
		"RESULT",
	}
	tw := out.NewTable(headers...)
	defer tw.Flush()
	for _, m := range movements {
		tw.PrintStructFields(m)
	}
}
