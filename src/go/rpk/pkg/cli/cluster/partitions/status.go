package partitions

import (
	"fmt"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/api/admin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newBalancerStatusCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "balancer-status",
		Short: "Queries cluster for partition balancer status",
		Long: `Queries cluster for partition balancer status:

If continuous partition balancing is enabled, redpanda will continuously
reassign partitions from both unavailable nodes and from nodes using more disk
space than the configured limit.

This command can be used to monitor the partition balancer status.

FIELDS

    Status:                        Either off, ready, starting, in progress, or
                                   stalled.
    Seconds Since Last Tick:       The last time the partition balancer ran.
    Current Reassignments Count:   Current number of partition reassignments in
                                   progress.
    Unavailable Nodes:             The nodes that have been unavailable after a
                                   time set by the
                                   "partition_autobalancing_node_availability_timeout_sec"
                                   cluster property.
    Over Disk Limit Nodes:         The nodes that surpassed the threshold of
                                   used disk percentage specified in the
                                   "partition_autobalancing_max_disk_usage_percent"
                                   cluster property.

BALANCER STATUS

    off:          The balancer is disabled.
    ready:        The balancer is active but there is nothing to do.
    starting:     The balancer is starting but has not run yet.
    in_progress:  The balancer is active and is in the process of scheduling
                  partition movements.
    stalled:      Violations have been detected and the balancer cannot correct
                  them.

STALLED BALANCER

A stalled balancer can occur for a few reasons and requires a bit of manual
investigation. A few areas to investigate:

* Are there are enough healthy nodes to which to move partitions? For example,
  in a three node cluster, no movements are possible for partitions with three
  replicas. You will see a stall every time there is a violation.

* Does the cluster have sufficient space? If all nodes in the cluster are
  utilizing more than 80% of their disk space, rebalancing cannot proceed.

* Do all partitions have quorum? If the majority of a partition's replicas are
  down, the partition cannot be moved.

* Are any nodes in maintenance mode? Partitions are not moved if any node is in
  maintenance mode.
`,
		Args: cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, _ []string) {
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			cl, err := admin.NewClient(fs, cfg)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			status, err := cl.GetPartitionStatus(cmd.Context())
			out.MaybeDie(err, "unable to request balancer status: %v", err)

			printBalancerStatus(status)
		},
	}

	return cmd
}

func printBalancerStatus(pbs admin.PartitionBalancerStatus) {
	const format = `Status:                       %v
Seconds Since Last Tick:      %v
Current Reassignment Count:   %v
`
	fmt.Printf(format, pbs.Status, pbs.SecondsSinceLastTick, pbs.CurrentReassignmentsCount)

	v := pbs.Violations
	if len(v.OverDiskLimitNodes) > 0 || len(v.UnavailableNodes) > 0 {
		const vFormat = `Unavailable Nodes:            %v
Over Disk Limit Nodes:        %v
`
		fmt.Printf(vFormat, v.UnavailableNodes, v.OverDiskLimitNodes)
	}
}
