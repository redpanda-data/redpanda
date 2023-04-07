package brokers

import (
	"errors"
	"strconv"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/api/admin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/types"
)

func newDecommissionBrokerStatus(fs afero.Fs, p *config.Params) *cobra.Command {
	var (
		completion int
		detailed   bool
	)
	cmd := &cobra.Command{
		Use:   "decommission-status [BROKER ID]",
		Short: "Show the progress of a node decommissioning",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			broker, _ := strconv.Atoi(args[0])
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			cl, err := admin.NewClient(fs, cfg)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			dbs, err := cl.DecommissionBrokerStatus(cmd.Context(), broker)
			if he := (*admin.HTTPResponseError)(nil); errors.As(err, &he) {
				// Special case 400 (validation) errors with friendly output
				// about the node is not decommissioning
				if he.Response.StatusCode == 400 {
					body, bodyErr := he.DecodeGenericErrorBody()
					if bodyErr == nil {
						out.Exit("%s", body.Message)
					}
				}
			}
			out.MaybeDie(err, "unable to request brokers: %v", err)

			if dbs.Finished {
				if dbs.ReplicasLeft == 0 {
					out.Exit("Note %d is decommissioned successfully.", broker)
				} else {
					out.Exit("Node %d is decommissioned but there are %d replicas left, which may be an issue inside Redpanda. Please describe how you encountered this at https://github.com/redpanda-data/redpanda/issues/new?assignees=&labels=kind%2Fbug&template=01_bug_report.md", broker, dbs.ReplicasLeft)
				}
			}

			headers := []string{"Namespace-Topic", "Partition", "Moving-to", "Completion-%", "Partition-size"}
			if detailed {
				headers = append(headers, "Bytes-moved", "Bytes-remaining")
			}
			f := func(p *admin.DecommissionPartitions) interface{} {
				nt := p.Ns + "/" + p.Topic
				if p.PartitionSize > 0 {
					completion = p.BytesMoved * 100 / p.PartitionSize
				}
				if detailed {
					return struct {
						NT             string
						Partition      int
						MovingTo       int
						Completion     int
						PartitionSize  int
						BytesMoved     int
						BytesRemaining int
					}{
						nt,
						p.Partition,
						p.MovingTo.NodeID,
						completion,
						p.PartitionSize,
						p.BytesMoved,
						p.BytesLeftToMove,
					}
				} else {
					return struct {
						NT            string
						Partition     int
						MovingTo      int
						Completion    int
						PartitionSize int
					}{
						nt,
						p.Partition,
						p.MovingTo.NodeID,
						completion,
						p.PartitionSize,
					}
				}
			}

			types.Sort(dbs.Partitions)

			tw := out.NewTable(headers...)
			defer tw.Flush()
			for _, p := range dbs.Partitions {
				tw.PrintStructFields(f(&p))
			}
		},
	}
	cmd.Flags().BoolVarP(&detailed, "detailed", "d", false, "Print how much data moved and remaining in bytes")

	return cmd
}
