package partitions

import (
	"fmt"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/api/admin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newMovementCancelCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var (
		node      int
		noConfirm bool
	)
	cmd := &cobra.Command{
		Use:   "movement-cancel",
		Short: "Cancel ongoing partition movements",
		Long: `Cancel ongoing partition movements.

By default, this command cancels all the partition movements in the cluster. 
To ensure that you do not accidentally cancel all partition movements, this 
command prompts users for confirmation before issuing the cancellation request. 
You can use "--no-confirm" to disable the confirmation prompt:

    rpk cluster partitions movement-cancel --no-confirm

If "--node" is set, this command will only stop the partition movements 
occurring in the specified node:

    rpk cluster partitions movement-cancel --node 1
`,
		Args: cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, args []string) {
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			cl, err := admin.NewClient(fs, cfg)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			var movements []admin.PartitionsMovementResult
			if node >= 0 {
				if !noConfirm {
					confirmed, err := out.Confirm("Confirm cancellation of partition movements in node %v?", node)
					out.MaybeDie(err, "unable to confirm partition movements cancel: %v", err)
					if !confirmed {
						out.Exit("Command execution canceled.")
					}
				}
				movements, err = cl.CancelNodePartitionsMovement(cmd.Context(), node)
				out.MaybeDie(err, "unable to cancel partition movements in node %v: %v", node, err)
			} else {
				if !noConfirm {
					confirmed, err := out.Confirm("Confirm cancellation of all partition movements in the cluster?")
					out.MaybeDie(err, "unable to confirm partition movements cancel: %v", err)
					if !confirmed {
						out.Exit("Command execution canceled.")
					}
				}
				movements, err = cl.CancelAllPartitionsMovement(cmd.Context())
				out.MaybeDie(err, "unable to cancel partition movements: %v", err)
			}

			if len(movements) == 0 {
				fmt.Println("There are no ongoing partition movements to cancel")
				return
			}
			printMovementsResult(movements)
		},
	}
	cmd.Flags().IntVar(&node, "node", -1, "ID of a specific node on which to cancel ongoing partition movements")
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
		result := struct {
			Namespace string
			Topic     string
			Partition int
			Result    string
		}{m.Namespace, m.Topic, m.Partition, m.Result}
		tw.PrintStructFields(result)
	}
}
