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
	var (
		node      int
		noConfirm bool
	)
	cmd := &cobra.Command{
		Use:   "movement-cancel",
		Short: "Cancel ongoing partitions reconfigurations",
		Long: `Cancel ongoing partitions reconfigurations happening in the cluster

By default, this command cancels all the partitions reconfiguration in the
cluster, to ensure that you do not accidentally cancel all movements, this
command prompts for confirmation before issuing the cancellation request to the
cluster, you can use "--no-confirm" to disable the confirmation prompt:

    rpk cluster partitions movement-cancel --no-confirm

If --node is set, this command will only stop the partitions happening in the
specified node:

    rpk cluster partitions movement-cancel --node 1
`,
		Args: cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			p := config.ParamsFromCommand(cmd)
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			cl, err := admin.NewClient(fs, cfg)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			var movements []admin.PartitionsMovementResult
			if node >= 0 {
				if !noConfirm {
					confirmed, err := out.Confirm("Confirm cancellation of partitions movement in node %v?", node)
					out.MaybeDie(err, "unable to confirm partitions movements cancel: %v", err)
					if !confirmed {
						out.Exit("Command execution canceled.")
					}
				}
				movements, err = cl.CancelNodePartitionsMovement(cmd.Context(), node)
				out.MaybeDie(err, "unable to cancel partitions movements in node %v: %v", node, err)
			} else {
				if !noConfirm {
					confirmed, err := out.Confirm("Confirm cancellation of all partitions movement in the cluster?")
					out.MaybeDie(err, "unable to confirm partitions movements cancel: %v", err)
					if !confirmed {
						out.Exit("Command execution canceled.")
					}
				}
				movements, err = cl.CancelAllPartitionsMovement(cmd.Context())
				out.MaybeDie(err, "unable to cancel partitions movements: %v", err)
			}

			if len(movements) == 0 {
				fmt.Println("There are no partitions movements ongoing to cancel")
				return
			}
			printMovementsResult(movements)
		},
	}
	cmd.Flags().IntVar(&node, "node", -1, "ID of the node you wish to stop all the ongoing partitions reconfigurations")
	cmd.Flags().BoolVar(&noConfirm, "no-confirm", false, "Disable confirmation prompt")
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
