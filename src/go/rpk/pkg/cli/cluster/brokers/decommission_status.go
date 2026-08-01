// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package brokers

import (
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"

	"github.com/docker/go-units"
	"github.com/redpanda-data/common-go/rpadmin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/types"
)

type decommissionStatusResponse struct {
	ReallocationFailures []reallocationFailure   `json:"reallocation_failures,omitempty" yaml:"reallocation_failures,omitempty"`
	AllocationFailures   []string                `json:"allocation_failures,omitempty" yaml:"allocation_failures,omitempty"`
	Partitions           []decommissionPartition `json:"partitions" yaml:"partitions"`
}

type reallocationFailure struct {
	Partition string `json:"partition" yaml:"partition"`
	Reason    string `json:"reason" yaml:"reason"`
}

type decommissionPartition struct {
	Partition         string `json:"partition" yaml:"partition"`
	MovingTo          int    `json:"moving_to" yaml:"moving_to"`
	CompletionPercent int    `json:"completion_percent" yaml:"completion_percent"`
	PartitionSize     int    `json:"partition_size" yaml:"partition_size"`
	BytesMoved        *int   `json:"bytes_moved,omitempty" yaml:"bytes_moved,omitempty"`
	BytesRemaining    *int   `json:"bytes_remaining,omitempty" yaml:"bytes_remaining,omitempty"`
}

func buildDecommissionStatus(dbs rpadmin.DecommissionStatusResponse, detailed bool) decommissionStatusResponse {
	resp := decommissionStatusResponse{
		Partitions: make([]decommissionPartition, 0, len(dbs.Partitions)),
	}

	if dbs.ReallocationFailureDetails != nil {
		resp.ReallocationFailures = make([]reallocationFailure, 0, len(dbs.ReallocationFailureDetails))
		for _, f := range dbs.ReallocationFailureDetails {
			ntp := f.NS + "/" + f.Topic + "/" + strconv.Itoa(f.Partition)
			resp.ReallocationFailures = append(resp.ReallocationFailures, reallocationFailure{
				Partition: ntp,
				Reason:    f.Error,
			})
		}
	} else if dbs.AllocationFailures != nil {
		resp.AllocationFailures = append([]string(nil), dbs.AllocationFailures...)
	}

	for _, p := range dbs.Partitions {
		ntp := p.Ns + "/" + p.Topic + "/" + strconv.Itoa(p.Partition)
		var completion int
		if p.PartitionSize > 0 {
			completion = p.BytesMoved * 100 / p.PartitionSize
		}
		dp := decommissionPartition{
			Partition:         ntp,
			MovingTo:          p.MovingTo.NodeID,
			CompletionPercent: completion,
			PartitionSize:     p.PartitionSize,
		}
		if detailed {
			dp.BytesMoved = &p.BytesMoved
			dp.BytesRemaining = &p.BytesLeftToMove
		}
		resp.Partitions = append(resp.Partitions, dp)
	}

	return resp
}

func printDecommissionStatus(f config.OutFormatter, resp decommissionStatusResponse, detailed, human bool, w io.Writer) {
	if isText, _, s, err := f.Format(resp); !isText {
		out.MaybeDie(err, "unable to print in the required format %q: %v", f.Kind, err)
		fmt.Fprintln(w, s)
		return
	}

	sizeFn := func(size int) string {
		if human {
			return units.HumanSize(float64(size))
		}
		return strconv.Itoa(size)
	}

	if len(resp.ReallocationFailures) > 0 {
		out.SectionTo(w, "reallocation failure details")
		tw := out.NewTableTo(w, "Partition", "Reason")
		for _, rf := range resp.ReallocationFailures {
			tw.Print(rf.Partition, rf.Reason)
		}
		tw.Flush()
		fmt.Fprintln(w)
	} else if len(resp.AllocationFailures) > 0 {
		out.SectionTo(w, "allocation failures")
		for _, af := range resp.AllocationFailures {
			fmt.Fprintln(w, af)
		}
		fmt.Fprintln(w)
	}

	out.SectionTo(w, "decommission progress")
	headers := []string{"Partition", "Moving-to", "Completion-%", "Partition-size"}
	if detailed {
		headers = append(headers, "Bytes-moved", "Bytes-remaining")
	}
	tw := out.NewTableTo(w, headers...)
	defer tw.Flush()
	for _, p := range resp.Partitions {
		if p.BytesMoved != nil && p.BytesRemaining != nil {
			tw.Print(p.Partition, p.MovingTo, p.CompletionPercent, sizeFn(p.PartitionSize), sizeFn(*p.BytesMoved), sizeFn(*p.BytesRemaining))
		} else {
			tw.Print(p.Partition, p.MovingTo, p.CompletionPercent, sizeFn(p.PartitionSize))
		}
	}
}

// NewDecommissionBrokerStatus returns the cluster brokers decommission-status command.
func NewDecommissionBrokerStatus(fs afero.Fs, p *config.Params) *cobra.Command {
	var (
		detailed  bool
		human     bool
		wait      bool
		watchOpts watchDecommissionOptions
	)
	cmd := &cobra.Command{
		Use:   "decommission-status [BROKER ID]",
		Short: "Show the progress of a node decommissioning",
		Long: `Show the progress of a node decommissioning.

When a node is being decommissioned, this command reports the decommissioning
progress as follows, where PARTITION-SIZE is in bytes. Using -H, it prints the
partition size in a human-readable format.

$ rpk cluster brokers decommission-status 4
DECOMMISSION PROGRESS
=====================
PARTITION                      MOVING-TO  COMPLETION-%  PARTITION-SIZE
kafka/test/0                   3          9             1699470920
kafka/test/4                   3          0             1614258779
kafka/test2/3                  3          3             2722706514
kafka/test2/4                  3          4             2945518089
kafka_internal/id_allocator/0  3          0             3562

Using --detailed / -d, it additionally prints granular reports.

$ rpk cluster brokers decommission-status 4 -d
DECOMMISSION PROGRESS
=====================
PARTITION      MOVING-TO  COMPLETION-%  PARTITION-SIZE  BYTES-MOVED  BYTES-REMAINING
kafka/test/0   3          13            1731773243      228114727    1503658516
kafka/test/4   3          1             1645952961      18752660     1627200301
kafka/test2/3  3          5             2752632301      140975805    2611656496
kafka/test2/4  3          6             2975443783      181581219    2793862564

If a partition cannot be moved for some reason, the command reports the
problematic partition in the 'REALLOCATION FAILURE DETAILS' or 'ALLOCATION FAILURES'
section and decommission fails. Typical scenarios for failure include:
there are no brokers that have enough space to allocate a partition, or that can satisfy
rack constraints, etc.

REALLOCATION FAILURE DETAILS
============================
PARTITION    REASON
kafka/foo/1  Missing partition size information, all replicas may be offline
kafka/foo/7  Missing partition size information, all replicas may be offline
`,
		Args: cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			f := p.Formatter
			if h, ok := f.Help(decommissionStatusResponse{}); ok {
				out.Exit(h)
			}

			broker, _ := strconv.Atoi(args[0])
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			config.CheckExitCloudAdmin(p)

			cl, err := adminapi.NewClient(cmd.Context(), fs, p)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			if wait {
				watchOpts.detailed = detailed
				watchOpts.human = human
				err = watchDecommission(cmd.Context(), cl, broker, f, watchOpts, cmd.OutOrStdout())
				out.MaybeDieErr(err)
				return
			}

			dbs, err := cl.DecommissionBrokerStatus(cmd.Context(), broker)
			if he := (*rpadmin.HTTPResponseError)(nil); errors.As(err, &he) {
				// Special case 400 (validation) errors with friendly output
				// about the node is not decommissioning
				if he.Response.StatusCode == 400 {
					body, bodyErr := he.DecodeGenericErrorBody()
					if bodyErr == nil {
						if isText, _, t, err := f.Format(decommissionStatusResponse{Partitions: []decommissionPartition{}}); !isText {
							out.MaybeDie(err, "unable to print in the requested format %q: %v", f.Kind, err)
							fmt.Fprintln(cmd.OutOrStdout(), t)
							return
						}
						out.Exit("%s", body.Message)
					}
				}
			}
			out.MaybeDie(err, "unable to request brokers: %v", err)

			if dbs.Finished {
				out.MaybeDieErr(printDecommissionFinished(f, dbs, broker, detailed, cmd.OutOrStdout()))
				return
			}

			types.Sort(dbs.Partitions)
			sort.Strings(dbs.AllocationFailures)

			resp := buildDecommissionStatus(dbs, detailed)
			printDecommissionStatus(f, resp, detailed, human, cmd.OutOrStdout())
		},
	}
	cmd.Flags().BoolVarP(&detailed, "detailed", "d", false, "Print how much data moved and remaining in bytes")
	cmd.Flags().BoolVarP(&human, "human-readable", "H", false, "Print the partition size in a human-readable form")
	installWaitFlags(cmd, &watchOpts, &wait)
	p.InstallFormatFlag(cmd)

	return cmd
}
