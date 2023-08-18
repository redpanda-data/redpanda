package brokers

import (
	"errors"
	"fmt"
	"sort"
	"strconv"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
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
		Long: `Show the progrss of a node decommissioning.

When a node is being decommissioned, this command reports the decommissioning
progress as follows, where PARTITION-SIZE is in bytes.

$ rpk redpanda admin brokers decommission-status 4
DECOMMISSION PROGRESS
=====================
NAMESPACE-TOPIC              PARTITION  MOVING-TO  COMPLETION-%  PARTITION-SIZE
kafka/test                   0          3          9             1699470920
kafka/test                   4          3          0             1614258779
kafka/test2                  3          3          3             2722706514
kafka/test2                  4          3          4             2945518089
kafka_internal/id_allocator  0          3          0             3562

Using --detailed / -d, it additionally prints granular reports.

$ rpk redpanda admin brokers decommission-status 4 -d
DECOMMISSION PROGRESS
=====================
NAMESPACE-TOPIC  PARTITION  MOVING-TO  COMPLETION-%  PARTITION-SIZE  BYTES-MOVED  BYTES-REMAINING
kafka/test       0          3          13            1731773243      228114727    1503658516
kafka/test       4          3          1             1645952961      18752660     1627200301
kafka/test2      3          3          5             2752632301      140975805    2611656496
kafka/test2      4          3          6             2975443783      181581219    2793862564

If a partition cannot be moved with some reason, the command reports the
problematic partition in the 'ALLOCATION FAILURES' section and decommission
never succeeds. Typical scenarios the failure occurs are; there's no node
that has enough space to allocate a partition or that can satisfy rack
constraints, etc.

ALLOCATION FAILURES
==================
kafka/foo/2
kafka/test/0

Note that the command reports allocation failed partitions only when
'partition_autobalancing_mode' is set to 'continuous'. See the current value
using 'rpk cluster config get partition_autobalancing_mode'.
`,
		Args: cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			broker, _ := strconv.Atoi(args[0])
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "unable to load config: %v", err)
			out.CheckExitCloudAdmin(p)

			cl, err := adminapi.NewClient(fs, p)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			dbs, err := cl.DecommissionBrokerStatus(cmd.Context(), broker)
			if he := (*adminapi.HTTPResponseError)(nil); errors.As(err, &he) {
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

			if dbs.AllocationFailures != nil {
				sort.Strings(dbs.AllocationFailures)
				out.Section("allocation failures")
				for _, f := range dbs.AllocationFailures {
					fmt.Println(f)
				}
				fmt.Println()
			}

			out.Section("decommission progress")
			headers := []string{"Namespace-Topic", "Partition", "Moving-to", "Completion-%", "Partition-size"}
			if detailed {
				headers = append(headers, "Bytes-moved", "Bytes-remaining")
			}
			f := func(p *adminapi.DecommissionPartitions) interface{} {
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
