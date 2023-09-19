package partitions

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/docker/go-units"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/types"
)

func newListPartitionMovementsCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var (
		completion       int
		all              bool
		human            bool
		partitions       []string
		response         []adminapi.ReconfigurationsResponse
		filteredResponse []adminapi.ReconfigurationsResponse
	)
	cmd := &cobra.Command{
		Use:   "move-status",
		Short: "Show ongoing partition movements",
		Long:  helpListMovement,
		Run: func(cmd *cobra.Command, topics []string) {
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "unable to load config: %v", err)
			out.CheckExitCloudAdmin(p)

			// If partition(s) is specified but no topic(s) is specified, exit.
			if len(topics) <= 0 && len(partitions) > 0 {
				out.Die("specify at least one topic when --partition is used, exiting.")
			}

			cl, err := adminapi.NewClient(fs, p)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			response, err = cl.Reconfigurations(cmd.Context())
			out.MaybeDie(err, "unable to list partition movements: %v\n", err)

			if len(response) == 0 {
				out.Exit("There are no ongoing partition movements.")
			}

			for _, t := range topics {
				nt := strings.Split(t, "/")
				if len(nt) > 2 {
					fmt.Printf("invalid format for topic %s, skipping.\n", t)
					continue
				}
				for _, r := range response {
					isKafkaNs := len(nt) == 1 && r.Ns == "kafka" && r.Topic == t
					isInternalNs := len(nt) == 2 && r.Ns == nt[0] && r.Topic == nt[1]

					if isKafkaNs || isInternalNs {
						if len(partitions) == 0 || contains(partitions, strconv.Itoa(r.PartitionID)) {
							filteredResponse = append(filteredResponse, r)
						}
					}
				}
			}
			if len(filteredResponse) > 0 {
				response = filteredResponse
			}

			sizeFn := func(size int) string {
				if human {
					return units.HumanSize(float64(size))
				}
				return strconv.Itoa(size)
			}

			f := func(rr *adminapi.ReconfigurationsResponse) interface{} {
				var (
					newReplica []int
					oldReplica []int
				)
				nt := rr.Ns + "/" + rr.Topic
				if rr.PartitionSize > 0 {
					completion = rr.BytesMoved * 100 / rr.PartitionSize
				}
				for _, r := range rr.NewReplicas {
					newReplica = append(newReplica, r.NodeID)
				}
				for _, r := range rr.PreviousReplicas {
					oldReplica = append(oldReplica, r.NodeID)
				}
				return struct {
					NT             string
					PartitionID    int
					MovingFrom     []int
					MovingTo       []int
					Completion     int
					PartitionSize  string
					BytesMoved     string
					BytesRemaining string
				}{
					nt,
					rr.PartitionID,
					oldReplica,
					newReplica,
					completion,
					sizeFn(rr.PartitionSize),
					sizeFn(rr.BytesMoved),
					sizeFn(rr.BytesLeft),
				}
			}

			types.Sort(response)

			out.Header("Partition movements", true, true, func() {
				headers := []string{"Namespace-Topic", "Partition", "Moving-from", "Moving-to", "Completion-%", "Partition-size", "Bytes-moved", "Bytes-remaining"}
				tw := out.NewTable(headers...)
				defer tw.Flush()
				for _, tps := range response {
					tw.PrintStructFields(f(&tps))
				}
			})

			out.Header("Reconciliation statuses", all, true, func() {
				var j int
				for _, p := range response {
					fmt.Printf("%s\n", p.Ns+"/"+p.Topic+"/"+strconv.Itoa(p.PartitionID))
					headers := []string{"Node-id", "Core", "Type", "Retry-number", "Revision", "Status"}
					tw := out.NewTable(headers...)
					for _, rs := range p.ReconciliationStatuses {
						var row []interface{}
						row = append(row, rs.NodeID)
						for _, s := range rs.Operations {
							row = append(row, s.Core, s.Type, s.RetryNumber, s.Revision, s.Status)
						}
						tw.Print(row...)
					}
					tw.Flush()
					j++
					if j < len(response) {
						fmt.Println()
					}
				}
			})
		},
	}

	cmd.Flags().BoolVarP(&all, "print-all", "a", false, "Print internal states about movements for debugging")
	cmd.Flags().BoolVarP(&human, "human-readable", "H", false, "Print the partition size in a human-readable form")
	cmd.Flags().StringSliceVarP(&partitions, "partition", "p", nil, "Partitions to print the ongoing movements (repeatable)")

	return cmd
}

// This function returns true when a partition that movement is
// ongoing is a requested partition by the --partition option.
func contains(pReq []string, pRes string) bool {
	for _, p := range pReq {
		if p == pRes {
			return true
		}
	}
	return false
}

const helpListMovement = `Show ongoing partition movements.

By default this command lists all the ongoing partition movements in the cluster.
Topics can be specified to print the move status of specific topics. By default,
this command assumes the "kafka" namespace, but you can use a "namespace/" to
specify internal namespaces.

    rpk cluster partitions move-status
    rpk cluster partitions move-status foo bar kafka_internal/tx

The "--partition / -p" flag can be used with topics to additional filter
requested partitions:

    rpk cluster partitions move-status foo bar --partition 0,1,2

The output contains the following columns, where PARTITION-SIZE is in bytes.
Using -H, it prints the partition size in a human-readable format

    NAMESPACE-TOPIC
    PARTITION
    MOVING-FROM
    MOVING-TO
    COMPLETION-%
    PARTITION-SIZE
    BYTES-MOVED
    BYTES-REMAINING

Using "--print-all / -a" the command additionally prints "RECONCILIATION STATUSES"
which reveals internal states on how the ongoing reconciliations work for debugging
purposes. That is, reported errors don't necessarily mean real problems.
`
